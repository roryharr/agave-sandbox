use {
    crate::{
        ArchiveFormat, Result, SnapshotArchiveKind, error::ArchiveSnapshotPackageError, paths,
        snapshot_archive_info::SnapshotArchiveInfo, snapshot_hash::SnapshotHash,
    },
    agave_fs::buffered_writer::large_file_buf_writer,
    log::info,
    solana_accounts_db::accounts_db::AccountsDb,
    solana_clock::Slot,
    solana_measure::measure::Measure,
    solana_metrics::datapoint_info,
    std::{fs, io::Write, path::Path},
};

/// Archives a snapshot into `archive_path`
pub fn archive_snapshot(
    snapshot_archive_kind: SnapshotArchiveKind,
    snapshot_slot: Slot,
    snapshot_hash: SnapshotHash,
    base_slot: Option<Slot>,
    accounts_db: &AccountsDb,
    bank_snapshot_dir: impl AsRef<Path>,
    archive_path: impl AsRef<Path>,
    archive_format: ArchiveFormat,
) -> Result<SnapshotArchiveInfo> {
    use ArchiveSnapshotPackageError as E;
    const ACCOUNTS_DIR: &str = "accounts";
    info!("Generating snapshot archive for slot {snapshot_slot}, kind: {snapshot_archive_kind:?}");

    let mut timer = Measure::start("snapshot_package-package_snapshots");
    let tar_dir = archive_path
        .as_ref()
        .parent()
        .expect("Tar output path is invalid");

    fs::create_dir_all(tar_dir).map_err(|err| E::CreateArchiveDir(err, tar_dir.to_path_buf()))?;

    // Create the staging directories
    let staging_dir_prefix = paths::TMP_SNAPSHOT_ARCHIVE_PREFIX;
    let staging_dir = tempfile::Builder::new()
        .prefix(&format!("{staging_dir_prefix}{snapshot_slot}-"))
        .tempdir_in(tar_dir)
        .map_err(|err| E::CreateStagingDir(err, tar_dir.to_path_buf()))?;
    let staging_snapshots_dir = staging_dir.path().join(paths::BANK_SNAPSHOTS_DIR);

    let slot_str = snapshot_slot.to_string();
    let staging_snapshot_dir = staging_snapshots_dir.join(&slot_str);
    // Creates staging snapshots/<slot>/
    fs::create_dir_all(&staging_snapshot_dir)
        .map_err(|err| E::CreateSnapshotStagingDir(err, staging_snapshot_dir.clone()))?;

    // To be a source for symlinking and archiving, the path need to be an absolute path
    let src_snapshot_dir = bank_snapshot_dir.as_ref().canonicalize().map_err(|err| {
        E::CanonicalizeSnapshotSourceDir(err, bank_snapshot_dir.as_ref().to_path_buf())
    })?;
    let staging_snapshot_file = staging_snapshot_dir.join(&slot_str);
    let src_snapshot_file = src_snapshot_dir.join(slot_str);
    symlink::symlink_file(&src_snapshot_file, &staging_snapshot_file)
        .map_err(|err| E::SymlinkSnapshot(err, src_snapshot_file, staging_snapshot_file))?;

    // Following the existing archive format, the status cache is under snapshots/, not under <slot>/
    // like in the snapshot dir.
    let staging_status_cache = staging_snapshots_dir.join(paths::SNAPSHOT_STATUS_CACHE_FILENAME);
    let src_status_cache = src_snapshot_dir.join(paths::SNAPSHOT_STATUS_CACHE_FILENAME);
    symlink::symlink_file(&src_status_cache, &staging_status_cache)
        .map_err(|err| E::SymlinkStatusCache(err, src_status_cache, staging_status_cache))?;

    // The bank snapshot has the version file, so symlink it to the correct staging path
    let staging_version_file = staging_dir.path().join(paths::SNAPSHOT_VERSION_FILENAME);
    let src_version_file = src_snapshot_dir.join(paths::SNAPSHOT_VERSION_FILENAME);
    symlink::symlink_file(&src_version_file, &staging_version_file).map_err(|err| {
        E::SymlinkVersionFile(err, src_version_file, staging_version_file.clone())
    })?;

    // Tar the staging directory into the archive at `staging_archive_path`
    let staging_archive_path = tar_dir.join(format!(
        "{}{}.{}",
        staging_dir_prefix,
        snapshot_slot,
        archive_format.extension(),
    ));

    {
        let archive_writer = large_file_buf_writer(&staging_archive_path)
            .map_err(|err| E::CreateArchiveFile(err, staging_archive_path.clone()))?;

        let do_archive_files = |encoder: &mut dyn Write| -> std::result::Result<(), E> {
            let mut archive = tar::Builder::new(encoder);
            // Disable sparse file handling.  This seems to be the root cause of an issue when
            // upgrading v2.0 to v2.1, and the tar crate from 0.4.41 to 0.4.42.
            // Since the tarball will still go through compression (zstd/etc) afterwards, disabling
            // sparse handling in the tar itself should be fine.
            //
            // Likely introduced in [^1].  Tracking resolution in [^2].
            // [^1] https://github.com/alexcrichton/tar-rs/pull/375
            // [^2] https://github.com/alexcrichton/tar-rs/issues/403
            archive.sparse(false);
            // Serialize the version and snapshots files before accounts so we can quickly determine the version
            // and other bank fields. This is necessary if we want to interleave unpacking with reconstruction
            archive
                .append_path_with_name(&staging_version_file, paths::SNAPSHOT_VERSION_FILENAME)
                .map_err(E::ArchiveVersionFile)?;
            archive
                .append_dir_all(paths::BANK_SNAPSHOTS_DIR, &staging_snapshots_dir)
                .map_err(E::ArchiveSnapshotsDir)?;

            accounts_db
                .write_snapshot_archive_entries(
                    base_slot,
                    snapshot_slot,
                    |filename, reader, len| {
                        let path_in_archive = Path::new(ACCOUNTS_DIR).join(filename);
                        let mut header = tar::Header::new_gnu();
                        header.set_path(&path_in_archive)?;
                        header.set_size(len);
                        header.set_cksum();
                        archive.append(&header, reader)?;
                        Ok(())
                    },
                )
                .map_err(E::ArchiveStorages)?;

            archive.into_inner().map_err(E::FinishArchive)?;
            Ok(())
        };

        match archive_format {
            ArchiveFormat::TarZstd { config } => {
                let mut encoder =
                    zstd::stream::Encoder::new(archive_writer, config.compression_level)
                        .map_err(E::CreateEncoder)?;
                do_archive_files(&mut encoder)?;
                encoder.finish().map_err(E::FinishEncoder)?;
            }
            ArchiveFormat::TarLz4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .level(1)
                    .build(archive_writer)
                    .map_err(E::CreateEncoder)?;
                do_archive_files(&mut encoder)?;
                let (_output, result) = encoder.finish();
                result.map_err(E::FinishEncoder)?;
            }
        };
    }

    // Atomically move the archive into position for other validators to find
    let metadata = fs::metadata(&staging_archive_path)
        .map_err(|err| E::QueryArchiveMetadata(err, staging_archive_path.clone()))?;
    let archive_path = archive_path.as_ref().to_path_buf();
    fs::rename(&staging_archive_path, &archive_path)
        .map_err(|err| E::MoveArchive(err, staging_archive_path, archive_path.clone()))?;

    timer.stop();
    info!(
        "Successfully created {}. slot: {}, elapsed ms: {}, size: {}",
        archive_path.display(),
        snapshot_slot,
        timer.as_ms(),
        metadata.len()
    );

    datapoint_info!(
        "archive-snapshot-package",
        ("slot", snapshot_slot, i64),
        ("archive_format", archive_format.to_string(), String),
        ("duration_ms", timer.as_ms(), i64),
        (
            if snapshot_archive_kind == SnapshotArchiveKind::Full {
                "full-snapshot-archive-size"
            } else {
                "incremental-snapshot-archive-size"
            },
            metadata.len(),
            i64
        ),
    );
    Ok(SnapshotArchiveInfo {
        path: archive_path,
        slot: snapshot_slot,
        hash: snapshot_hash,
        archive_format,
    })
}

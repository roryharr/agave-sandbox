use {
    crate::AccountsFile::{AppendVec, TieredStorage},
    ahash::HashSet,
    clap::{
        crate_description, crate_name, value_t_or_exit, values_t_or_exit, App, AppSettings, Arg,
        ArgMatches, SubCommand,
    },
    rayon::prelude::*,
    solana_accounts_db::{
        account_storage::stored_account_info::StoredAccountInfoWithoutData,
        accounts_file::{AccountsFile, StorageAccess},
    },
    solana_pubkey::Pubkey,
    std::{
        fs, io,
        mem::ManuallyDrop,
        num::Saturating,
        path::{Path, PathBuf},
    },
};

const CMD_INSPECT: &str = "inspect";
const CMD_SEARCH: &str = "search";

fn main() {
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .global_setting(AppSettings::ArgRequiredElseHelp)
        .global_setting(AppSettings::ColoredHelp)
        .global_setting(AppSettings::InferSubcommands)
        .global_setting(AppSettings::UnifiedHelpMessage)
        .global_setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name(CMD_INSPECT)
                .about("Inspects an account storage file and display each account's information")
                .arg(
                    Arg::with_name("path")
                        .index(1)
                        .takes_value(true)
                        .value_name("PATH")
                        .help("Account storage file to inspect"),
                )
                .arg(
                    Arg::with_name("verbose")
                        .short("v")
                        .long("verbose")
                        .takes_value(false)
                        .help("Show additional account information"),
                ),
        )
        .subcommand(
            SubCommand::with_name(CMD_SEARCH)
                .about("Searches for accounts")
                .arg(
                    Arg::with_name("path")
                        .index(1)
                        .takes_value(true)
                        .value_name("PATH")
                        .help("Account storage directory to search"),
                )
                .arg(
                    Arg::with_name("addresses")
                        .index(2)
                        .takes_value(true)
                        .value_name("PUBKEYS")
                        .value_delimiter(",")
                        .help("Search for the entries of one or more pubkeys, delimited by commas"),
                )
                .arg(
                    Arg::with_name("verbose")
                        .short("v")
                        .long("verbose")
                        .takes_value(false)
                        .help("Show additional account information"),
                ),
        )
        .get_matches();

    let subcommand = matches.subcommand();
    let subcommand_str = subcommand.0.to_string();
    match subcommand {
        (CMD_INSPECT, Some(subcommand_matches)) => cmd_inspect(&matches, subcommand_matches),
        (CMD_SEARCH, Some(subcommand_matches)) => cmd_search(&matches, subcommand_matches),
        _ => unreachable!(),
    }
    .unwrap_or_else(|err| {
        eprintln!("Error: '{subcommand_str}' failed: {err}");
        std::process::exit(1);
    });
}

fn cmd_inspect(
    _app_matches: &ArgMatches<'_>,
    subcommand_matches: &ArgMatches<'_>,
) -> Result<(), String> {
    let path = value_t_or_exit!(subcommand_matches, "path", String);
    let verbose = subcommand_matches.is_present("verbose");
    do_inspect(path, verbose)
}

fn cmd_search(
    _app_matches: &ArgMatches<'_>,
    subcommand_matches: &ArgMatches<'_>,
) -> Result<(), String> {
    let path = value_t_or_exit!(subcommand_matches, "path", String);
    let addresses = values_t_or_exit!(subcommand_matches, "addresses", Pubkey);
    let addresses = HashSet::from_iter(addresses);
    let verbose = subcommand_matches.is_present("verbose");
    do_search(path, addresses, verbose)
}

fn log_account_info(
    storage: &AccountsFile,
    offset: usize,
    account: &StoredAccountInfoWithoutData,
    verbose: bool,
) {
    if verbose {
        match storage {
            AppendVec(vec) => {
                vec.get_stored_account_meta_callback(offset, |account| {
                    println!("{account:?}");
                });
            }
            TieredStorage(_) => println!("unimplemented verbose output for TieredStorage"),
        }
    } else {
        println!(
            "{:#x}: {:44}, owner: {:44}, data size: {:}, lamports: {}",
            offset, account.pubkey, account.owner, account.data_len, account.lamports,
        );
    }
}

fn do_inspect(file: impl AsRef<Path>, verbose: bool) -> Result<(), String> {
    let file_size = fs::metadata(&file)
        .map_err(|err| {
            format!(
                "failed to get file metadata '{}': {err}",
                file.as_ref().display(),
            )
        })?
        .len() as usize;

    let (storage, _size) =
        AccountsFile::new_from_file(file.as_ref(), file_size, StorageAccess::default()).map_err(
            |err| {
                format!(
                    "failed to open account storage file '{}': {err}",
                    file.as_ref().display(),
                )
            },
        )?;
    // By default, when the storage is dropped, the backing file will be removed.
    // We do not want to remove the backing file here in the store-tool, so prevent dropping.
    let storage = ManuallyDrop::new(storage);

    let mut num_accounts = Saturating(0usize);
    let mut stored_accounts_size = Saturating(0);
    let mut lamports = Saturating(0);
    storage
        .scan_accounts_without_data(|offset, account| {
            log_account_info(&storage, offset, &account, verbose);
            num_accounts += 1;
            stored_accounts_size += storage.calculate_stored_size(account.data_len);
            lamports += account.lamports;
        })
        .map_err(|err| {
            format!(
                "failed to scan accounts in file '{}': {err}",
                file.as_ref().display(),
            )
        })?;

    println!(
        "number of accounts: {}, stored accounts size: {}, file size: {}, lamports: {}",
        num_accounts,
        stored_accounts_size,
        storage.capacity(),
        lamports,
    );
    Ok(())
}

fn do_search(
    dir: impl AsRef<Path>,
    addresses: HashSet<Pubkey>,
    verbose: bool,
) -> Result<(), String> {
    fn get_files_in(dir: impl AsRef<Path>) -> Result<Vec<PathBuf>, io::Error> {
        let mut files = Vec::new();
        let entries = fs::read_dir(dir)?;
        for entry in entries {
            let path = entry?.path();
            if path.is_file() {
                let path = fs::canonicalize(path)?;
                files.push(path);
            }
        }
        Ok(files)
    }

    let files = get_files_in(&dir).map_err(|err| {
        format!(
            "failed to get files in dir '{}': {err}",
            dir.as_ref().display(),
        )
    })?;
    files.par_iter().for_each(|file| {
        let file_size = match fs::metadata(file) {
            Ok(metadata) => metadata.len() as usize,
            Err(err) => {
                eprintln!("failed to get storage metadata '{}': {err}", file.display(),);
                return;
            }
        };
        let Ok((storage, _size)) =
            AccountsFile::new_from_file(file, file_size, StorageAccess::default()).inspect_err(
                |err| {
                    eprintln!(
                        "failed to open account storage file '{}': {err}",
                        file.display(),
                    )
                },
            )
        else {
            return;
        };
        // By default, when the storage is dropped, the backing file will be removed.
        // We do not want to remove the backing file here in the store-tool, so prevent dropping.
        let storage = ManuallyDrop::new(storage);

        let file_name = Path::new(file.file_name().expect("path is a file"));
        storage
            .scan_accounts_without_data(|offset, account| {
                if addresses.contains(account.pubkey()) {
                    println!("storage file: {}", file_name.display());
                    log_account_info(&storage, offset, &account, verbose);
                }
            })
            .unwrap_or_else(|err| {
                eprintln!(
                    "failed to scan accounts in file '{}': {err}",
                    file.display()
                )
            });
    });

    Ok(())
}

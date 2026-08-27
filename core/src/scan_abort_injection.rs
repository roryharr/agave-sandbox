//! TEMPORARY validation harness — DELETE BEFORE MERGE.
//!
//! `remove_unrooted_slots` only runs when ReplayStage dumps a fork, which is far too rare to
//! validate "abort scans promptly when their bank is removed" against on a live node. This drives
//! the same path on demand: start a full accounts scan on an unrooted bank, ask
//! `BankForksController` to clear that bank, and time how long the bank stays alive afterwards.
//! The scan thread holds `Arc<Bank>` exactly as an RPC scan does, so the bank cannot drop until
//! the scan releases it.
//!
//! Enable with `AGAVE_SCAN_ABORT_INJECT=<seconds between injections>`. Non-voting nodes only:
//! every injection makes the node repair and replay the dumped slots again.

use {
    log::*,
    solana_metrics::datapoint_info,
    solana_runtime::{bank_forks::BankForks, bank_forks_controller::BankForksController},
    std::{
        env,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{Builder, JoinHandle, sleep},
        time::{Duration, Instant},
    },
};

const ENV_VAR: &str = "AGAVE_SCAN_ABORT_INJECT";
/// Abandon an injection if the scan hasn't reached its first account by now.
const SCAN_START_TIMEOUT: Duration = Duration::from_secs(30);
/// Stop waiting for the cleared bank to drop.
const DROP_TIMEOUT: Duration = Duration::from_secs(600);
const POLL_INTERVAL: Duration = Duration::from_millis(1);
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub fn spawn(
    bank_forks: Arc<RwLock<BankForks>>,
    bank_forks_controller: Arc<dyn BankForksController>,
    exit: Arc<AtomicBool>,
) -> Option<JoinHandle<()>> {
    let raw_interval = env::var(ENV_VAR).ok()?;
    let Ok(seconds) = raw_interval.parse::<u64>() else {
        warn!("{ENV_VAR} must be a number of seconds, got {raw_interval:?}; not injecting");
        return None;
    };
    if seconds == 0 {
        warn!("{ENV_VAR} must be greater than zero; not injecting");
        return None;
    }
    let interval = Duration::from_secs(seconds);
    warn!("{ENV_VAR} is set: clearing a bank under an active scan every {interval:?}");

    Builder::new()
        .name("solScanInject".to_string())
        .spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                sleep_until_exit(&exit, interval);
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                inject_once(&bank_forks, bank_forks_controller.as_ref(), &exit);
            }
        })
        .ok()
}

fn inject_once(
    bank_forks: &RwLock<BankForks>,
    bank_forks_controller: &dyn BankForksController,
    exit: &AtomicBool,
) {
    // `BankForks::slots_to_clear` skips anything at or below the root and pulls in descendants, so
    // any unrooted slot is a valid target and a root advancing past it is a no-op rather than a
    // panic.
    let (bank, root) = {
        let bank_forks = bank_forks.read().unwrap();
        (bank_forks.working_bank(), bank_forks.root())
    };
    let (slot, bank_id) = (bank.slot(), bank.bank_id());
    if slot <= root {
        info!("scan abort injection: working bank {slot} is rooted (root {root}), skipping");
        return;
    }

    // Hand the scan thread the only `Arc` outside `BankForks` and watch the bank through a `Weak`,
    // so waiting for the drop here doesn't itself keep the bank alive.
    let weak_bank = Arc::downgrade(&bank);
    let scan_reached_first_account = Arc::new(AtomicBool::new(false));
    let scan_thread = {
        let scan_reached_first_account = scan_reached_first_account.clone();
        Builder::new()
            .name("solScanInjectScan".to_string())
            .spawn(move || {
                let mut accounts_visited = 0u64;
                let scan_started = Instant::now();
                let result = bank.scan_all_accounts(|account| {
                    if account.is_some() {
                        accounts_visited += 1;
                        scan_reached_first_account.store(true, Ordering::Relaxed);
                    }
                });
                let scan_elapsed = scan_started.elapsed();
                // release the bank before the thread is joined, so `weak_bank` is all that's left
                drop(bank);
                (scan_elapsed, accounts_visited, result)
            })
            .expect("spawn scan thread")
    };

    // A mark that lands before the scan registers is rejected by `ScanGuard::try_new` and measures
    // nothing, so wait until the scan is genuinely inside its loop.
    let wait_started = Instant::now();
    while !scan_reached_first_account.load(Ordering::Relaxed) {
        if wait_started.elapsed() > SCAN_START_TIMEOUT || exit.load(Ordering::Relaxed) {
            warn!("scan abort injection: scan on slot {slot} did not start, abandoning injection");
            let _ = scan_thread.join();
            return;
        }
        sleep(POLL_INTERVAL);
    }

    let clear_requested = Instant::now();
    if let Err(err) = bank_forks_controller.clear_bank(slot) {
        warn!("scan abort injection: clear_bank({slot}) failed: {err:?}");
        let _ = scan_thread.join();
        return;
    }
    // `clear_bank` returns once ReplayStage has serviced it, so the mark landed somewhere inside
    // this window; report it so it can be subtracted from the numbers below.
    let clear_elapsed = clear_requested.elapsed();

    let (scan_elapsed, accounts_visited, scan_result) =
        scan_thread.join().expect("scan thread panicked");
    let scan_returned_after_clear = clear_requested.elapsed();

    // The bank drops only once every holder releases it, so a drop far behind the scan's return
    // means something other than this scan was pinning the fork and the sample says nothing about
    // the change.
    let mut bank_dropped_after_clear = None;
    while clear_requested.elapsed() < DROP_TIMEOUT {
        if weak_bank.upgrade().is_none() {
            bank_dropped_after_clear = Some(clear_requested.elapsed());
            break;
        }
        sleep(POLL_INTERVAL);
    }

    let scan_result = match &scan_result {
        Ok(()) => "ok".to_string(),
        Err(err) => format!("{err:?}"),
    };
    info!(
        "scan abort injection: slot {slot} bank id {bank_id}: scan visited {accounts_visited} \
         accounts in {scan_elapsed:?} and returned {scan_result}; clear_bank took \
         {clear_elapsed:?}; scan returned {scan_returned_after_clear:?} after the clear was \
         requested; bank dropped {bank_dropped_after_clear:?} after"
    );
    datapoint_info!(
        "scan_abort_injection",
        ("slot", slot as i64, i64),
        ("accounts_visited", accounts_visited as i64, i64),
        ("scan_total_us", scan_elapsed.as_micros() as i64, i64),
        ("clear_bank_us", clear_elapsed.as_micros() as i64, i64),
        (
            "scan_return_after_clear_us",
            scan_returned_after_clear.as_micros() as i64,
            i64
        ),
        (
            "bank_drop_after_clear_us",
            bank_dropped_after_clear
                .map(|elapsed| elapsed.as_micros() as i64)
                .unwrap_or(-1),
            i64
        ),
        ("scan_result", scan_result, String),
    );
}

fn sleep_until_exit(exit: &AtomicBool, duration: Duration) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline && !exit.load(Ordering::Relaxed) {
        sleep(EXIT_POLL_INTERVAL);
    }
}

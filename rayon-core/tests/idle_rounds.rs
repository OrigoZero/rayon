use rayon_core::ThreadPoolBuilder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// One burst of the work a frame hands a pool: a scope of tiny tasks, a join
/// tree and a broadcast. Returns the tasks it ran.
fn burst(pool: &rayon_core::ThreadPool) -> usize {
    let ran = AtomicUsize::new(0);
    pool.scope(|s| {
        for _ in 0..pool.current_num_threads() * 2 {
            s.spawn(|_| {
                ran.fetch_add(1, Ordering::Relaxed);
            });
        }
    });
    fn tree(depth: u32, ran: &AtomicUsize) {
        if depth == 0 {
            ran.fetch_add(1, Ordering::Relaxed);
            return;
        }
        rayon_core::join(|| tree(depth - 1, ran), || tree(depth - 1, ran));
    }
    pool.install(|| tree(4, &ran));
    for n in pool.broadcast(|_| 1usize) {
        ran.fetch_add(n, Ordering::Relaxed);
    }
    ran.load(Ordering::Relaxed)
}

#[test]
#[cfg_attr(any(target_os = "emscripten", target_family = "wasm"), ignore)]
fn workers_that_sleep_after_no_idle_rounds_still_take_every_burst() {
    let threads = 4;
    let pool = ThreadPoolBuilder::new()
        .num_threads(threads)
        .idle_rounds(0)
        .build()
        .unwrap();
    let per_burst = threads * 2 + 16 + threads;
    for i in 0..400 {
        assert_eq!(burst(&pool), per_burst, "burst {i} lost work");
        if i % 4 == 0 {
            // Long enough for every worker to fall asleep before the next burst.
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

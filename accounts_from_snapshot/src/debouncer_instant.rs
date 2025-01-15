use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct Debouncer {
    base: Instant,
    cooldown_ms: i64,
    last: AtomicI64,
}

impl Debouncer {
    pub fn new(cooldown: Duration) -> Self {
        Self {
            // need some reference in the past
            base: Instant::now() - Duration::from_secs(100_000),
            cooldown_ms: cooldown.as_millis() as i64,
            last: AtomicI64::new(0),
        }
    }
    pub fn can_fire(&self) -> bool {
        let passed_total_ms = (Instant::now() - self.base).as_millis() as i64;

        let results = self
            .last
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |last| {
                if passed_total_ms - last > self.cooldown_ms {
                    Some(passed_total_ms)
                } else {
                    None
                }
            });

        results.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use crate::debouncer_instant::Debouncer;
    use std::sync::Arc;
    use std::thread;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn fire() {
        let debouncer = Debouncer::new(Duration::from_millis(500));

        assert!(debouncer.can_fire());
        assert!(!debouncer.can_fire());
        sleep(Duration::from_millis(200));
        assert!(!debouncer.can_fire());
        sleep(Duration::from_millis(400));
        assert!(debouncer.can_fire());
    }

    #[test]
    fn threading() {
        let debouncer = Debouncer::new(Duration::from_millis(500));

        thread::spawn(move || {
            debouncer.can_fire();
        });
    }

    #[test]
    fn shared() {
        let debouncer = Arc::new(Debouncer::new(Duration::from_millis(500)));

        let debouncer_copy = debouncer.clone();
        thread::spawn(move || {
            debouncer_copy.can_fire();
        });

        let debouncer_copy = debouncer.clone();
        thread::spawn(move || {
            debouncer_copy.can_fire();
        });
    }
}

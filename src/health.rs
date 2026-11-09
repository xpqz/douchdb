//! The cached filesystem part of `/_health`. Depends on nothing else in the
//! crate, so the node state can hold it.

/// How long the filesystem part of the health check is reused. `/_health` is
/// unauthenticated, so it must not touch the disk on every request.
pub(crate) const FS_HEALTH_TTL: std::time::Duration = std::time::Duration::from_secs(2);

/// Result of the blocking filesystem checks.
#[derive(Clone, Copy)]
pub(crate) struct FsHealth {
    pub(crate) readable: bool,
    pub(crate) writable: bool,
    pub(crate) unreadable: u64,
}

/// Short-lived cache of [`FsHealth`].
#[derive(Default)]
pub(crate) struct FsHealthCache(std::sync::Mutex<Option<(std::time::Instant, FsHealth)>>);

impl FsHealthCache {
    pub(crate) fn fresh(&self, now: std::time::Instant) -> Option<FsHealth> {
        let slot = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        match *slot {
            Some((at, value)) if now.saturating_duration_since(at) < FS_HEALTH_TTL => Some(value),
            _ => None,
        }
    }

    pub(crate) fn store(&self, now: std::time::Instant, value: FsHealth) {
        *self.0.lock().unwrap_or_else(|poison| poison.into_inner()) = Some((now, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn fs_health_cache_serves_within_the_ttl_only() {
        let cache = FsHealthCache::default();
        let start = Instant::now();
        assert!(cache.fresh(start).is_none());
        cache.store(
            start,
            FsHealth {
                readable: true,
                writable: true,
                unreadable: 3,
            },
        );
        let hit = cache.fresh(start + FS_HEALTH_TTL - Duration::from_millis(1));
        assert_eq!(hit.map(|value| value.unreadable), Some(3));
        assert!(cache.fresh(start + FS_HEALTH_TTL).is_none());
    }
}

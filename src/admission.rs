//! Per-principal admission control.
//!
//! This is deliberately not a general-purpose scheduler. It is the small
//! fairness layer between authentication and work: a global in-flight bound, a
//! per-principal in-flight bound, a request-rate token bucket, and separate
//! heavy/background permits. Excess work is rejected with 429 instead of being
//! queued without bound.

use crate::auth::Principal;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    Normal,
    Heavy,
    Background,
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_in_flight: usize,
    pub per_user_in_flight: usize,
    pub per_user_rate: usize,
    pub heavy_in_flight: usize,
    pub heavy_per_user: usize,
    pub background_in_flight: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_in_flight: 64,
            per_user_in_flight: 16,
            per_user_rate: 0,
            heavy_in_flight: 8,
            heavy_per_user: 1,
            background_in_flight: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denied {
    Rate,
    User,
    Global,
    Heavy,
    Background,
}

impl Denied {
    pub fn reason(self) -> &'static str {
        match self {
            Denied::Rate => "request rate exceeded for this user",
            Denied::User => "too many concurrent requests for this user",
            Denied::Global => "server is at its in-flight request limit",
            Denied::Heavy => "too many concurrent view/search requests",
            Denied::Background => "background work is already running",
        }
    }
}

#[derive(Debug)]
pub struct Permit {
    _global: Option<OwnedSemaphorePermit>,
    _user: Option<OwnedSemaphorePermit>,
    _heavy_global: Option<OwnedSemaphorePermit>,
    _heavy_user: Option<OwnedSemaphorePermit>,
}

struct UserState {
    normal: Arc<Semaphore>,
    heavy: Arc<Semaphore>,
    rate: Mutex<TokenBucket>,
}

struct TokenBucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(rate: usize) -> TokenBucket {
        let rate = rate as f64;
        TokenBucket {
            rate,
            burst: rate.max(1.0),
            tokens: rate.max(1.0),
            last: Instant::now(),
        }
    }

    fn try_take(&mut self, now: Instant) -> bool {
        if self.rate <= 0.0 {
            return true;
        }
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub struct Admission {
    limits: Limits,
    global: Arc<Semaphore>,
    heavy: Arc<Semaphore>,
    background: Arc<Semaphore>,
    users: Mutex<HashMap<String, Arc<UserState>>>,
}

impl Admission {
    pub fn new(limits: Limits) -> Admission {
        Admission {
            limits,
            global: semaphore(limits.max_in_flight),
            heavy: semaphore(limits.heavy_in_flight),
            background: semaphore(limits.background_in_flight),
            users: Mutex::new(HashMap::new()),
        }
    }

    fn user(&self, principal: &Principal) -> Arc<UserState> {
        let mut users = self
            .users
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        users
            .entry(principal.key().to_string())
            .or_insert_with(|| {
                Arc::new(UserState {
                    normal: semaphore(self.limits.per_user_in_flight),
                    heavy: semaphore(self.limits.heavy_per_user),
                    rate: Mutex::new(TokenBucket::new(self.limits.per_user_rate)),
                })
            })
            .clone()
    }

    /// Admit one request. `Lane::Background` uses the dedicated background
    /// permit and bypasses per-user accounting.
    pub fn try_admit(&self, principal: &Principal, lane: Lane) -> Result<Permit, Denied> {
        if lane == Lane::Background {
            return self.try_background();
        }
        let user = self.user(principal);
        // A no-auth / internal service principal has no user identity to share
        // fairly; keep the global and heavy-global bounds, but not the per-user
        // ones. The server remains bounded by `max_in_flight`.
        let service = principal.admin && principal.name.is_none();
        if !service {
            let mut rate = user
                .rate
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if !rate.try_take(Instant::now()) {
                return Err(Denied::Rate);
            }
        }

        let mut permit = Permit {
            _global: None,
            _user: None,
            _heavy_global: None,
            _heavy_user: None,
        };
        if lane == Lane::Heavy {
            permit._heavy_global = Some(acquire(&self.heavy).map_err(|_| Denied::Heavy)?);
            if !service {
                permit._heavy_user = Some(acquire(&user.heavy).map_err(|_| Denied::Heavy)?);
            }
        }
        if !service {
            permit._user = Some(acquire(&user.normal).map_err(|_| Denied::User)?);
        }
        permit._global = Some(acquire(&self.global).map_err(|_| Denied::Global)?);
        Ok(permit)
    }

    pub fn try_background(&self) -> Result<Permit, Denied> {
        Ok(Permit {
            _global: None,
            _user: None,
            _heavy_global: None,
            _heavy_user: Some(acquire(&self.background).map_err(|_| Denied::Background)?),
        })
    }

    /// Classify a request by path. Only view/search work gets a heavy permit;
    /// everything else is interactive.
    pub fn classify(path: &str) -> Lane {
        if path.contains("/_view/") || path.contains("/_search/") {
            Lane::Heavy
        } else {
            Lane::Normal
        }
    }

    /// A request that should not consume admission capacity: health checks and
    /// embedded static assets.
    pub fn exempt(path: &str) -> bool {
        path == "/_up"
            || path == "/_health"
            || path == "/favicon.ico"
            || path == "/_utils"
            || path.starts_with("/_utils/")
    }
}

fn semaphore(limit: usize) -> Arc<Semaphore> {
    let permits = if limit == 0 { usize::MAX / 2 } else { limit };
    Arc::new(Semaphore::new(permits))
}

fn acquire(semaphore: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, TryAcquireError> {
    semaphore.clone().try_acquire_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(name: &str) -> Principal {
        Principal {
            name: Some(name.to_string()),
            roles: vec![],
            admin: false,
        }
    }

    fn admission_with(max: usize, per_user: usize, heavy: usize, heavy_user: usize) -> Admission {
        Admission::new(Limits {
            max_in_flight: max,
            per_user_in_flight: per_user,
            per_user_rate: 0,
            heavy_in_flight: heavy,
            heavy_per_user: heavy_user,
            background_in_flight: 1,
        })
    }

    #[test]
    fn per_user_and_global_limits() {
        let admission = admission_with(2, 1, 1, 1);
        let alice = principal("alice");
        let bob = principal("bob");

        let first = admission.try_admit(&alice, Lane::Normal).unwrap();
        assert_eq!(
            admission.try_admit(&alice, Lane::Normal).unwrap_err(),
            Denied::User
        );
        assert!(admission.try_admit(&bob, Lane::Normal).is_ok());
        drop(first);
        assert!(admission.try_admit(&alice, Lane::Normal).is_ok());

        let global = admission_with(1, 2, 1, 1);
        let held = global.try_admit(&alice, Lane::Normal).unwrap();
        assert_eq!(
            global.try_admit(&bob, Lane::Normal).unwrap_err(),
            Denied::Global
        );
        drop(held);
        assert!(global.try_admit(&bob, Lane::Normal).is_ok());
    }

    #[test]
    fn heavy_lane_is_separate_from_interactive() {
        let admission = admission_with(4, 4, 1, 1);
        let alice = principal("alice");
        let bob = principal("bob");

        let heavy = admission.try_admit(&alice, Lane::Heavy).unwrap();
        assert_eq!(
            admission.try_admit(&bob, Lane::Heavy).unwrap_err(),
            Denied::Heavy
        );
        // Bob can still do interactive work while Alice's heavy request runs.
        let normal = admission.try_admit(&bob, Lane::Normal).unwrap();
        drop(heavy);
        drop(normal);
        assert!(admission.try_admit(&bob, Lane::Heavy).is_ok());
    }

    #[test]
    fn background_is_independent() {
        let admission = admission_with(1, 1, 1, 1);
        let _background = admission.try_background().unwrap();
        assert_eq!(admission.try_background().unwrap_err(), Denied::Background);
        // Interactive work is not blocked by background work.
        assert!(admission
            .try_admit(&principal("alice"), Lane::Normal)
            .is_ok());
    }

    #[test]
    fn classification_and_exemptions() {
        assert_eq!(Admission::classify("/db/_design/v/_view/rows"), Lane::Heavy);
        assert_eq!(
            Admission::classify("/db/_design/v/_search/rows"),
            Lane::Heavy
        );
        assert_eq!(Admission::classify("/db/_all_docs"), Lane::Normal);
        assert!(Admission::exempt("/_up"));
        assert!(Admission::exempt("/_health"));
        assert!(Admission::exempt("/_utils/app.js"));
        assert!(!Admission::exempt("/db/doc"));
    }

    #[test]
    fn token_bucket_refills() {
        let start = Instant::now();
        let mut bucket = TokenBucket {
            rate: 1.0,
            burst: 2.0,
            tokens: 1.0,
            last: start,
        };
        assert!(bucket.try_take(start));
        assert!(!bucket.try_take(start));
        assert!(bucket.try_take(start + std::time::Duration::from_secs(1)));
    }
}

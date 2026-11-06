//! The error type shared by storage, database and HTTP layers.

use serde_json::{json, Value};

#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub error: &'static str,
    pub reason: String,
    /// The error is a stored record that could not be decoded, as opposed to
    /// an I/O failure. Listings skip such a record; everything else fails.
    pub corrupt: bool,
}

impl ApiError {
    pub fn new(status: u16, error: &'static str, reason: impl Into<String>) -> ApiError {
        ApiError {
            status,
            error,
            reason: reason.into(),
            corrupt: false,
        }
    }

    /// A stored record that cannot be decoded: a 500 for a point read, a
    /// skipped row in a listing.
    pub fn corrupt(reason: impl Into<String>) -> ApiError {
        ApiError {
            corrupt: true,
            ..ApiError::new(500, "internal_server_error", reason)
        }
    }

    pub fn not_found(reason: impl Into<String>) -> ApiError {
        ApiError::new(404, "not_found", reason)
    }

    pub fn conflict() -> ApiError {
        ApiError::new(409, "conflict", "Document update conflict.")
    }

    pub fn bad_request(reason: impl Into<String>) -> ApiError {
        ApiError::new(400, "bad_request", reason)
    }

    pub fn body(&self) -> Value {
        json!({ "error": self.error, "reason": self.reason })
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error, self.reason)
    }
}

impl std::error::Error for ApiError {}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        ApiError::new(500, "internal_server_error", e.to_string())
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        if e.downcast_ref::<crate::store::CorruptRecord>().is_some() {
            return ApiError::corrupt(e.to_string());
        }
        ApiError::new(500, "internal_server_error", e.to_string())
    }
}

pub type DbResult<T> = std::result::Result<T, ApiError>;

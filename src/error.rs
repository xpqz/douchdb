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

    /// A failure that is the server's fault. Conversions from `serde_json`
    /// errors are deliberately absent: `?` on a user's JSON would turn a 400
    /// into this 500, so callers who serialise their own data opt in here.
    pub fn internal(reason: impl std::fmt::Display) -> ApiError {
        ApiError::new(500, "internal_server_error", reason.to_string())
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

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        if e.downcast_ref::<crate::store::CorruptRecord>().is_some() {
            return ApiError::corrupt(format!("{e:#}"));
        }
        ApiError::internal(format!("{e:#}"))
    }
}

/// The status and reason for a JavaScript or view-index failure with no
/// context of its own. Callers that name what failed (a map, reduce or
/// search index function) wrap the text themselves.
impl From<crate::js::JsError> for ApiError {
    fn from(e: crate::js::JsError) -> Self {
        use crate::js::JsError;
        match e {
            JsError::Index(reason) => ApiError::new(500, "view_index_error", reason),
            JsError::InvalidValue(reason) => ApiError::new(500, "invalid_value", reason),
            other => ApiError::new(500, "unknown_error", other.to_string()),
        }
    }
}

pub type DbResult<T> = std::result::Result<T, ApiError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn js_errors_keep_their_status_and_reason() {
        use crate::js::JsError;
        let index = ApiError::from(JsError::Index("lock held".to_string()));
        assert_eq!(
            (index.status, index.error, index.reason.as_str()),
            (500, "view_index_error", "lock held")
        );
        let invalid = ApiError::from(JsError::InvalidValue("bad _stats".to_string()));
        assert_eq!(
            (invalid.status, invalid.error, invalid.reason.as_str()),
            (500, "invalid_value", "bad _stats")
        );
        let other = ApiError::from(JsError::Runtime("Error: x".to_string()));
        assert_eq!(
            (other.status, other.error, other.reason.as_str()),
            (500, "unknown_error", "Error: x")
        );
    }

    #[test]
    fn anyhow_conversion_keeps_the_cause_chain() {
        let err = anyhow::anyhow!("disk full").context("writing shard 3");
        let api = ApiError::from(err);
        assert_eq!(api.status, 500);
        assert_eq!(api.reason, "writing shard 3: disk full");
    }
}

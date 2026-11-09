//! The rules for a database's name, shared by the node state and the HTTP
//! layer. Depends only on the error type.

use crate::error::ApiError;

/// CouchDB's answer to creating a database with an illegal name.
pub(crate) fn illegal_db_name(name: &str) -> ApiError {
    ApiError::new(
        400,
        "illegal_database_name",
        format!(
            "Name: '{name}'. Only lowercase characters (a-z), digits (0-9), and any of the \
             characters _, $, (, ), +, -, and / are allowed. Must begin with a letter."
        ),
    )
}

/// The system databases CouchDB's `couch_db:validate_dbname` accepts although
/// they do not match the name regex.
const SYSTEM_DB_NAMES: [&str; 3] = ["_users", "_replicator", "_global_changes"];

/// CouchDB's `^[a-z][a-z0-9_$()+/-]*(\.[0-9]{10,})?$` (`DBNAME_REGEX` in
/// `couch_db.erl`) applied after dropping one trailing `.couch`
/// (`couch_util:drop_dot_couch_ext`), or one of the system databases. The
/// optional digits are the shard timestamp; the clustered create runs the same
/// check, so `foo.couch` and `foo.1234567890` are legal names, stored as
/// given. Doubled and trailing slashes are legal.
pub(crate) fn valid_db_name(name: &str) -> bool {
    let bare = name.strip_suffix(".couch").unwrap_or(name);
    let body = match bare.rsplit_once('.') {
        Some((body, digits))
            if digits.len() >= 10 && digits.bytes().all(|b| b.is_ascii_digit()) =>
        {
            body
        }
        _ => bare,
    };
    let mut bytes = body.bytes();
    let regex_match = bytes.next().is_some_and(|b| b.is_ascii_lowercase())
        && bytes.all(|b| {
            b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || matches!(b, b'_' | b'-' | b'$' | b'+' | b'(' | b')' | b'/')
        });
    regex_match || SYSTEM_DB_NAMES.contains(&bare)
}

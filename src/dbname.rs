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

/// The system databases of CouchDB's `?SYSTEM_DATABASES`, which
/// `couch_db:is_system_db_name` recognises.
const SYSTEM_DB_NAMES: [&str; 3] = ["_users", "_replicator", "_global_changes"];

/// CouchDB's `DBNAME_REGEX` from `couch_db.erl`,
/// `^[a-z][a-z0-9_$()+/-]*(\.[0-9]{10,})?$`. The optional digits are the shard
/// timestamp.
fn matches_regex(name: &str) -> bool {
    let body = match name.rsplit_once('.') {
        Some((body, digits))
            if digits.len() >= 10 && digits.bytes().all(|b| b.is_ascii_digit()) =>
        {
            body
        }
        _ => name,
    };
    let mut bytes = body.bytes();
    bytes.next().is_some_and(|b| b.is_ascii_lowercase())
        && bytes.all(|b| {
            b.is_ascii_lowercase()
                || b.is_ascii_digit()
                || matches!(b, b'_' | b'-' | b'$' | b'+' | b'(' | b')' | b'/')
        })
}

/// `couch_db:is_system_db_name` on a name already stripped of `.couch`: the
/// last path component (`filename:basename`) is a system database and the
/// rest (`filename:dirname`), if any, itself matches the name regex, so
/// `ts.1234567890/_users` is a system database and `Foo/_users` is not.
fn is_system_db_name(name: &str) -> bool {
    let trimmed = name.trim_end_matches('/');
    match trimmed.rsplit_once('/') {
        None => SYSTEM_DB_NAMES.contains(&trimmed),
        Some((dir, base)) => {
            SYSTEM_DB_NAMES.contains(&base) && matches_regex(dir.trim_end_matches('/'))
        }
    }
}

/// CouchDB's `couch_db:validate_dbname`: the name regex applied after dropping
/// one trailing `.couch` (`couch_util:drop_dot_couch_ext`), or a system
/// database name, with or without a valid prefix. The clustered create runs
/// the same check, so `foo.couch` and `foo.1234567890` are legal names, stored
/// as given. Doubled and trailing slashes are legal.
pub(crate) fn valid_db_name(name: &str) -> bool {
    let bare = name.strip_suffix(".couch").unwrap_or(name);
    matches_regex(bare) || is_system_db_name(bare)
}

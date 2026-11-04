//! Database access control: `_security` objects, admin-only paths and the
//! write freeze during a rebalance.

use super::*;

fn forbidden() -> Response {
    json(
        StatusCode::FORBIDDEN,
        json!({"error": "forbidden", "reason": "You are not authorized to access this db."}),
    )
}

pub(crate) fn access_denied(principal: &Principal) -> Response {
    if principal.is_authenticated() {
        forbidden()
    } else {
        unauthorized()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DbAccess {
    Member,
    Admin,
}

fn security_matches(principal: &Principal, section: Option<&Value>) -> bool {
    let Some(section) = section.and_then(Value::as_object) else {
        return false;
    };
    if let (Some(name), Some(names)) = (
        principal.name.as_deref(),
        section.get("names").and_then(Value::as_array),
    ) {
        if names
            .iter()
            .any(|candidate| candidate.as_str() == Some(name))
        {
            return true;
        }
    }
    if let Some(roles) = section.get("roles").and_then(Value::as_array) {
        if roles.iter().any(|role| {
            role.as_str()
                .map(|role| principal.has_role(role))
                .unwrap_or(false)
        }) {
            return true;
        }
    }
    false
}

fn security_list_empty(security: &Value, key: &str) -> bool {
    let Some(section) = security.get(key).and_then(Value::as_object) else {
        return true;
    };
    ["names", "roles"].iter().all(|field| {
        section
            .get(*field)
            .and_then(Value::as_array)
            .map(Vec::is_empty)
            .unwrap_or(true)
    })
}

pub(super) fn db_access(st: &AppState, principal: &Principal, db: &Db) -> Option<DbAccess> {
    if principal.admin {
        return Some(DbAccess::Admin);
    }
    let security = db.security();
    if security_matches(principal, security.get("admins")) {
        return Some(DbAccess::Admin);
    }
    if security_matches(principal, security.get("members")) {
        return Some(DbAccess::Member);
    }
    // A database is public only when the whole security object is empty and
    // the operator chose `open`. This is deliberately stricter than CouchDB's
    // "empty members means public" rule: setting admins without members must
    // not accidentally expose the database to every authenticated user.
    if security_list_empty(&security, "members") {
        if security_list_empty(&security, "admins") && !st.config.default_admin_only {
            Some(DbAccess::Member)
        } else {
            None
        }
    } else {
        None
    }
}

pub(super) fn require_db_access(
    st: &AppState,
    principal: &Principal,
    db: &Db,
    need_admin: bool,
) -> Option<Response> {
    match db_access(st, principal, db) {
        Some(DbAccess::Admin) => None,
        Some(DbAccess::Member) if !need_admin => None,
        Some(DbAccess::Member) => Some(forbidden()),
        None => Some(access_denied(principal)),
    }
}

pub(super) fn still_allowed(st: &AppState, db: &Db, headers: &HeaderMap) -> bool {
    let principal = st.principal(headers);
    db_access(st, &principal, db).is_some()
}

pub(super) fn requires_db_admin(rest: &str, method: &Method) -> bool {
    if rest == "_security" {
        return *method == Method::PUT || *method == Method::POST;
    }
    if rest == "_compact"
        || rest == "_view_cleanup"
        || rest == "_search_cleanup"
        || rest == "_purge"
        || rest == "_sync_shards"
    {
        return true;
    }
    if rest == "_revs_limit" {
        return *method == Method::PUT || *method == Method::POST;
    }
    // A design document write is `_design/{ddoc}`; queries under `_view/` and
    // `_search/` remain member operations.
    if rest.starts_with("_design/")
        && !rest.contains("/_view/")
        && !rest.contains("/_search/")
        && !rest.contains("/_show/")
        && !rest.contains("/_list/")
        && !rest.contains("/_update/")
    {
        return method.as_str() == "COPY"
            || matches!(
                *method,
                Method::PUT | Method::POST | Method::DELETE | Method::PATCH
            );
    }
    false
}

fn is_write_request(method: &Method, rest: &str) -> bool {
    if method.as_str() == "COPY" || matches!(*method, Method::PUT | Method::DELETE) {
        return true;
    }
    if *method != Method::POST {
        return false;
    }
    // POST is also used for read-only query shapes.
    if rest == "_all_docs" || rest == "_bulk_get" || rest == "_revs_diff" {
        return false;
    }
    if rest.starts_with("_design/")
        && (rest.contains("/_view/")
            || rest.contains("/_search/")
            || rest.contains("/_search_info/"))
    {
        return false;
    }
    !matches!(rest, "_search_info" | "_index_info")
}

pub(super) fn reject_during_rebalance(
    st: &AppState,
    db: &Db,
    method: &Method,
    rest: &str,
    headers: &HeaderMap,
) -> Option<Response> {
    if db.rebalance_in_progress() && !is_forwarded(st, headers) && is_write_request(method, rest) {
        Some(fail(ApiError::new(
            503,
            "rebalance_in_progress",
            "writes are paused while this database changes shard placement",
        )))
    } else {
        None
    }
}

pub(super) fn validate_security(value: &Value) -> DbResult<()> {
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("security object must be an object"))?;
    for key in ["admins", "members"] {
        let Some(section) = object.get(key) else {
            continue;
        };
        let section = section
            .as_object()
            .ok_or_else(|| ApiError::bad_request(format!("`{key}` must be an object")))?;
        for field in ["names", "roles"] {
            let Some(list) = section.get(field) else {
                continue;
            };
            let list = list.as_array().ok_or_else(|| {
                ApiError::bad_request(format!("`{key}.{field}` must be an array"))
            })?;
            if !list.iter().all(Value::is_string) {
                return Err(ApiError::bad_request(format!(
                    "`{key}.{field}` must contain strings"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn unauthorized() -> Response {
    json(
        StatusCode::UNAUTHORIZED,
        json!({"error": "unauthorized", "reason": "You are not authorized to access this db."}),
    )
}

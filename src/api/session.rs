//! `/_session` cookie authentication.

use super::*;

pub async fn session_get(Caller(principal): Caller, headers: HeaderMap) -> Response {
    let had_credentials =
        headers.contains_key(header::AUTHORIZATION) || headers.contains_key(header::COOKIE);
    if had_credentials && !principal.is_authenticated() {
        return unauthorized();
    }
    let authenticated = principal.is_authenticated();
    let name = match &principal.name {
        Some(name) => json!(name),
        None if authenticated => json!("admin"),
        None => Value::Null,
    };
    let mut info = json!({
        "authentication_db": "_users",
        "authentication_handlers": ["cookie", "default", "bearer"]
    });
    // CouchDB reports the handler that authenticated the request, and omits
    // the key for an anonymous one (`couch_httpd_auth:handle_session_req/2`).
    // An Authorization header takes precedence over a cookie, as in
    // `Auth::principal`.
    if authenticated {
        let handler = match headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        {
            Some(value) if value.starts_with("Bearer ") => Some("bearer"),
            Some(_) => Some("default"),
            None if headers.contains_key(header::COOKIE) => Some("cookie"),
            None => None,
        };
        if let Some(handler) = handler {
            info["authenticated"] = json!(handler);
        }
    }
    json(
        StatusCode::OK,
        json!({
            "ok": true,
            "userCtx": { "name": name, "roles": principal.roles },
            "info": info
        }),
    )
}

/// CouchDB clears the session cookie with DELETE /_session. The cookie is
/// stateless, so expiring it on the client is all that is needed.
pub async fn session_delete() -> Response {
    let mut response = json(StatusCode::OK, json!({"ok": true}));
    response.headers_mut().insert(
        header::SET_COOKIE,
        "AuthSession=; Version=1; Path=/; Max-Age=0; HttpOnly; SameSite=Strict"
            .parse()
            .expect("valid cookie"),
    );
    response
}

pub async fn session_post(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let text = String::from_utf8_lossy(&body);
    let mut user = String::new();
    let mut pass = String::new();
    if let Ok(v) = serde_json::from_slice::<Value>(&body) {
        user = v
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        pass = v
            .get("password")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
    }
    if user.is_empty() {
        for pair in text.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            match percent_decode(k).as_str() {
                "name" => user = percent_decode(v),
                "password" => pass = percent_decode(v),
                _ => {}
            }
        }
    }
    let principal = match &st.ctx.auth {
        Some(auth) => auth.authenticate(&user, &pass),
        None => Some(Principal {
            name: Some(if user.is_empty() {
                "admin".into()
            } else {
                user.clone()
            }),
            roles: vec!["_admin".to_string()],
            admin: true,
        }),
    };
    let Some(principal) = principal else {
        return json(
            StatusCode::UNAUTHORIZED,
            json!({"error": "unauthorized", "reason": "Name or password is incorrect."}),
        );
    };
    let name = principal.name.clone().unwrap_or_else(|| user.clone());
    let mut response = json(
        StatusCode::OK,
        json!({
            "ok": true,
            "name": name,
            "roles": principal.roles,
        }),
    );
    if let Some(auth) = &st.ctx.auth {
        let cookie_name = principal.name.as_deref().unwrap_or(&user);
        if let Ok(value) = auth.cookie_header_for(cookie_name).parse() {
            response.headers_mut().insert(header::SET_COOKIE, value);
        }
    }
    response
}

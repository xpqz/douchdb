//! Authentication and principals: server admin, token users and sessions.
//!
//! The users file is deliberately small and self-contained. It is not a
//! replicated `_users` database; it is a deployment-owned list of tokens and
//! roles. Passwords are not stored: a token is used as the Basic/session
//! password. `token_sha256` is supported so the file itself need not contain
//! bearer secrets.

use axum::http::HeaderMap;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const COOKIE: &str = "AuthSession";
const TTL_SECS: u64 = 60 * 60 * 24;

/// The authenticated caller. `admin` is the server-admin flag; `roles` are the
/// user's roles. `name=None` is anonymous.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Principal {
    pub name: Option<String>,
    pub roles: Vec<String>,
    pub admin: bool,
}

impl Principal {
    pub fn service_admin() -> Principal {
        Principal {
            name: None,
            roles: vec!["_admin".to_string()],
            admin: true,
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.name.is_some() || self.admin
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.admin || self.roles.iter().any(|candidate| candidate == role)
    }
}

#[derive(Clone, Debug)]
struct User {
    name: String,
    token: Option<String>,
    token_sha256: Option<[u8; 32]>,
    roles: Vec<String>,
    admin: bool,
    /// Digest of the user's credentials, mixed into the cookie key.
    salt: [u8; 32],
}

impl User {
    fn matches_token(&self, token: &str) -> bool {
        // An empty credential never authenticates, whatever the entry holds.
        if token.is_empty() {
            return false;
        }
        if let Some(expected) = &self.token {
            if constant_time_eq(expected.as_bytes(), token.as_bytes()) {
                return true;
            }
        }
        if let Some(expected) = self.token_sha256 {
            let digest = Sha256::digest(token.as_bytes());
            return constant_time_eq(&expected, &digest);
        }
        false
    }
}

/// Shortest accepted plaintext user token. Tokens are the only credential, so
/// a trivially guessable one is a startup error rather than a quiet hole.
const MIN_TOKEN_LEN: usize = 8;

const SECRET_FILE: &str = ".session-secret";

/// The key that signs `AuthSession` cookies.
///
/// `--session-secret` wins when set. Otherwise a random key is generated once
/// and kept in the data directory, readable by the owner only, so cookies
/// survive restarts; CouchDB likewise persists a generated
/// `couch_httpd_auth/secret`. The key is never derived from the admin
/// credentials (a cookie holder could brute-force the password offline) or
/// from the cluster secret (which is often a shared, documented default).
pub fn session_secret(data: &Path, configured: &str) -> Result<Vec<u8>, String> {
    if !configured.trim().is_empty() {
        return Ok(configured.as_bytes().to_vec());
    }
    let path = data.join(SECRET_FILE);
    let describe = |error: std::io::Error| format!("session secret {}: {error}", path.display());
    loop {
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                return decode_secret(text.trim())
                    .map_err(|error| format!("session secret {}: {error}", path.display()))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(describe(error)),
        }
        let key = crate::util::random_bytes::<32>();
        match crate::db::create_private_atomic(&path, crate::util::hex(&key).as_bytes()) {
            Ok(()) => return Ok(key.to_vec()),
            // Another process created it first; read theirs.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(describe(error)),
        }
    }
}

fn decode_secret(text: &str) -> Result<Vec<u8>, String> {
    if text.len() != 64 {
        return Err("expected 64 hex characters; delete the file to generate a new one".into());
    }
    let mut out = Vec::with_capacity(32);
    for index in 0..32 {
        let pair = text
            .get(index * 2..index * 2 + 2)
            .ok_or_else(|| "not hex; delete the file to generate a new one".to_string())?;
        out.push(
            u8::from_str_radix(pair, 16)
                .map_err(|_| "not hex; delete the file to generate a new one".to_string())?,
        );
    }
    Ok(out)
}

pub struct Auth {
    pub user: String,
    pub pass: String,
    users: Vec<User>,
    secret: Vec<u8>,
}

impl Auth {
    /// A server-admin-only auth instance. Kept for callers that only have
    /// `--admin`. `secret` is the session cookie HMAC key; see
    /// [`session_secret`].
    pub fn new(user: String, pass: String, secret: Vec<u8>) -> Auth {
        Auth {
            user,
            pass,
            users: Vec::new(),
            secret,
        }
    }

    /// Load a users file. `admin` is optional; when present, the config admin
    /// continues to work alongside file users. A file user may not share the
    /// config admin's name: a session cookie carries only the name, so it
    /// would resolve to the admin.
    pub fn from_users_file(
        admin: Option<(String, String)>,
        path: &Path,
        secret: Vec<u8>,
    ) -> Result<Auth, String> {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("users file {}: {error}", path.display()))?;
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("users file {}: {error}", path.display()))?;
        let entries = value
            .get("users")
            .and_then(Value::as_array)
            .ok_or_else(|| "users file must contain a `users` array".to_string())?;
        let mut users: Vec<User> = Vec::with_capacity(entries.len());
        for entry in entries {
            let name = entry
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| "each user needs a non-empty `name`".to_string())?
                .to_string();
            if users.iter().any(|user| user.name == name) {
                return Err(format!("duplicate user `{name}`"));
            }
            if admin.as_ref().is_some_and(|(user, _)| *user == name) {
                return Err(format!(
                    "user `{name}` has the same name as the --admin user"
                ));
            }
            let token = entry
                .get("token")
                .and_then(Value::as_str)
                .map(str::to_string);
            let token_sha256 = entry
                .get("token_sha256")
                .and_then(Value::as_str)
                .map(decode_hex_32)
                .transpose()
                .map_err(|error| format!("user `{name}`: {error}"))?;
            if token.is_none() && token_sha256.is_none() {
                return Err(format!("user `{name}` needs `token` or `token_sha256`"));
            }
            if token
                .as_deref()
                .is_some_and(|token| token.chars().count() < MIN_TOKEN_LEN)
            {
                return Err(format!(
                    "user `{name}`: `token` must be at least {MIN_TOKEN_LEN} characters"
                ));
            }
            if token_sha256 == Some(Sha256::digest(b"").into()) {
                return Err(format!(
                    "user `{name}`: `token_sha256` is the hash of an empty token"
                ));
            }
            let roles: Vec<String> = entry
                .get("roles")
                .and_then(Value::as_array)
                .map(|roles| {
                    roles
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let admin = entry.get("admin").and_then(Value::as_bool).unwrap_or(false)
                || roles.iter().any(|role| role == "_admin");
            let salt = credential_salt(token.as_deref(), token_sha256.as_ref());
            users.push(User {
                name,
                token,
                token_sha256,
                roles,
                admin,
                salt,
            });
        }
        let (user, pass) = admin.unwrap_or_default();
        Ok(Auth {
            user,
            pass,
            users,
            secret,
        })
    }

    fn principal_for_user(&self, user: &User) -> Principal {
        let mut roles = user.roles.clone();
        if user.admin && !roles.iter().any(|role| role == "_admin") {
            roles.push("_admin".to_string());
        }
        Principal {
            name: Some(user.name.clone()),
            roles,
            admin: user.admin,
        }
    }

    fn principal_for_name(&self, name: &str) -> Option<Principal> {
        if !self.user.is_empty() && name == self.user {
            return Some(Principal {
                name: Some(name.to_string()),
                roles: vec!["_admin".to_string()],
                admin: true,
            });
        }
        self.users
            .iter()
            .find(|user| user.name == name)
            .map(|user| self.principal_for_user(user))
    }

    fn token_principal(&self, token: &str) -> Option<Principal> {
        self.users
            .iter()
            .find(|user| user.matches_token(token))
            .map(|user| self.principal_for_user(user))
    }

    fn password_principal(&self, name: &str, password: &str) -> Option<Principal> {
        if password.is_empty() {
            return None;
        }
        if !self.user.is_empty()
            && constant_time_eq(name.as_bytes(), self.user.as_bytes())
            && constant_time_eq(password.as_bytes(), self.pass.as_bytes())
        {
            return Some(Principal {
                name: Some(name.to_string()),
                roles: vec!["_admin".to_string()],
                admin: true,
            });
        }
        self.users
            .iter()
            .find(|user| user.name == name && user.matches_token(password))
            .map(|user| self.principal_for_user(user))
    }

    /// Authenticate a name/token pair for `_session`.
    pub fn authenticate(&self, name: &str, password: &str) -> Option<Principal> {
        self.password_principal(name, password)
    }

    pub fn principal(&self, headers: &HeaderMap) -> Principal {
        if let Some(value) = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        {
            if let Some(token) = value.strip_prefix("Bearer ") {
                if let Some(principal) = self.token_principal(token.trim()) {
                    return principal;
                }
            }
            if let Some(principal) = self.basic_principal(value) {
                return principal;
            }
            // An Authorization header that does not authenticate is not
            // downgraded to anonymous.
            return Principal::default();
        }
        let cookies = headers
            .get(axum::http::header::COOKIE)
            .and_then(|value| value.to_str().ok());
        if let Some(name) = self.cookie_user(cookies) {
            if let Some(principal) = self.principal_for_name(&name) {
                return principal;
            }
        }
        Principal::default()
    }

    fn basic_principal(&self, header: &str) -> Option<Principal> {
        let encoded = header.strip_prefix("Basic ")?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .ok()?;
        let text = String::from_utf8(decoded).ok()?;
        let (name, password) = text.split_once(':')?;
        self.password_principal(name, password)
    }

    /// Compatibility wrapper used by the old auth tests.
    #[cfg(test)]
    pub fn check_basic(&self, header: &str) -> bool {
        self.basic_principal(header).is_some()
    }

    /// The credential digest that keys `name`'s cookies, or `None` for an
    /// unknown user.
    fn cookie_salt(&self, name: &str) -> Option<[u8; 32]> {
        if !self.user.is_empty() && name == self.user {
            return Some(Sha256::digest(self.pass.as_bytes()).into());
        }
        self.users
            .iter()
            .find(|user| user.name == name)
            .map(|user| user.salt)
    }

    /// The cookie HMAC: keyed by the session secret plus the user's credential
    /// digest, as CouchDB keys it with the secret plus the user's salt, so
    /// changing a credential invalidates the user's sessions.
    fn mac(&self, salt: &[u8; 32]) -> Hmac<Sha256> {
        let mut key = self.secret.clone();
        key.extend_from_slice(salt);
        Hmac::<Sha256>::new_from_slice(&key).expect("hmac key")
    }

    fn sign(&self, name: &str, payload: &str) -> String {
        let mut mac = self.mac(&self.cookie_salt(name).unwrap_or_default());
        mac.update(payload.as_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    }

    #[cfg(test)]
    pub fn cookie_header(&self) -> String {
        self.cookie_header_for(&self.user)
    }

    pub fn cookie_header_for(&self, name: &str) -> String {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + TTL_SECS;
        let payload = format!("{name}:{exp}");
        let token = format!(
            "{}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.as_bytes()),
            self.sign(name, &payload)
        );
        format!("{COOKIE}={token}; Version=1; Path=/; HttpOnly; SameSite=Strict")
    }

    fn cookie_user(&self, header: Option<&str>) -> Option<String> {
        let header = header?;
        for part in header.split(';') {
            let Some((name, value)) = part.trim().split_once('=') else {
                continue;
            };
            if name != COOKIE {
                continue;
            }
            let Some((payload_b64, sig)) = value.split_once('.') else {
                continue;
            };
            let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload_b64)
                .ok()?;
            let payload = String::from_utf8(payload).ok()?;
            let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(sig)
                .ok()?;
            let Some((user, exp)) = payload.rsplit_once(':') else {
                continue;
            };
            let Some(salt) = self.cookie_salt(user) else {
                continue;
            };
            let mut mac = self.mac(&salt);
            mac.update(payload.as_bytes());
            if mac.verify_slice(&signature).is_err() {
                continue;
            }
            let Ok(exp) = exp.parse::<u64>() else {
                continue;
            };
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if exp > now {
                return Some(user.to_string());
            }
        }
        None
    }

    /// Compatibility wrapper used by the old auth tests.
    #[cfg(test)]
    pub fn check_cookies(&self, header: Option<&str>) -> bool {
        self.cookie_user(header).is_some()
    }
}

/// Digest of a user's credentials, so a rotated token changes it.
fn credential_salt(token: Option<&str>, token_sha256: Option<&[u8; 32]>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(token.unwrap_or_default().as_bytes());
    hasher.update([0]);
    if let Some(digest) = token_sha256 {
        hasher.update(digest);
    }
    hasher.finalize().into()
}

fn decode_hex_32(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64 {
        return Err("`token_sha256` must be 64 hex characters".to_string());
    }
    let mut out = [0u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        // `get` rather than indexing: 64 bytes of non-ASCII text would put a
        // slice boundary inside a character.
        let pair = value
            .get(index * 2..index * 2 + 2)
            .ok_or_else(|| "`token_sha256` must be hex".to_string())?;
        *byte =
            u8::from_str_radix(pair, 16).map_err(|_| "`token_sha256` must be hex".to_string())?;
    }
    Ok(out)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests run in parallel, so each one needs its own users file. Sharing a
    // fixed path lets one test read the file while another has truncated it.
    fn users_auth(name: &str) -> Auth {
        Auth::from_users_file(
            Some(("admin".into(), "secret".into())),
            &std::env::temp_dir().join(name),
            b"session-secret".to_vec(),
        )
        .expect("users")
    }

    fn write_users(name: &str) {
        std::fs::write(
            std::env::temp_dir().join(name),
            r#"{"users":[
                {"name":"alice","token":"alice-token","roles":["analyst"]},
                {"name":"root","token":"root-token","roles":["_admin"]},
                {"name":"hashed","token_sha256":"2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"}
            ]}"#,
        )
        .unwrap();
    }

    fn data_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("douchdb-{name}-{}", crate::util::uuid()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn configured_session_secret_is_used_verbatim() {
        let dir = data_dir("auth-flag");
        let key = session_secret(&dir, "from-the-flag").unwrap();
        assert_eq!(key, b"from-the-flag");
        // An explicit secret does not create the file.
        assert!(!dir.join(SECRET_FILE).exists());
        let auth = Auth::new("admin".into(), "pw".into(), key);
        let other = Auth::new("admin".into(), "pw".into(), b"other".to_vec());
        let cookie = auth.cookie_header();
        let value = cookie.split(';').next().unwrap();
        assert!(auth.check_cookies(Some(value)));
        assert!(!other.check_cookies(Some(value)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn generated_session_secret_persists_and_ignores_credentials() {
        let dir = data_dir("auth-gen");
        let first = session_secret(&dir, "").unwrap();
        let second = session_secret(&dir, "  ").unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 32);
        assert_ne!(first, b"admin:secret:douchdb");
        let auth = Auth::new("admin".into(), "secret".into(), first);
        let again = Auth::new("admin".into(), "secret".into(), second);
        // A cookie signed with the formerly derived key is rejected.
        let legacy = Auth::new(
            "admin".into(),
            "secret".into(),
            b"admin:secret:douchdb".to_vec(),
        );
        let legacy_cookie = legacy.cookie_header();
        let legacy_value = legacy_cookie.split(';').next().unwrap();
        assert!(!auth.check_cookies(Some(legacy_value)));
        // A cookie from the first instance still works after "restart".
        let cookie = auth.cookie_header();
        assert!(again.check_cookies(Some(cookie.split(';').next().unwrap())));
        // A different data directory gets a different key.
        let elsewhere = data_dir("auth-gen2");
        assert_ne!(
            session_secret(&elsewhere, "").unwrap(),
            session_secret(&dir, "").unwrap()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.join(SECRET_FILE))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&elsewhere);
    }

    #[test]
    fn corrupt_session_secret_file_is_an_error() {
        let dir = data_dir("auth-bad");
        std::fs::write(dir.join(SECRET_FILE), "nonsense").unwrap();
        assert!(session_secret(&dir, "").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn basic_auth() {
        let auth = Auth::new("admin".into(), "secret".into(), b"k".to_vec());
        let header = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("admin:secret")
        );
        assert!(auth.check_basic(&header));
        let wrong = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("admin:nope")
        );
        assert!(!auth.check_basic(&wrong));
    }

    #[test]
    fn cookie_roundtrip() {
        let auth = Auth::new("admin".into(), "secret".into(), b"k".to_vec());
        let header = auth.cookie_header();
        assert!(header.contains("SameSite=Strict"));
        let value = header.split(';').next().unwrap();
        assert!(auth.check_cookies(Some(value)));
        assert!(!auth.check_cookies(Some("AuthSession=forged")));
        assert!(!auth.check_cookies(None));
    }

    #[test]
    fn token_users_and_hashes() {
        let file = "douchdb-auth-test-token-users.json";
        write_users(file);
        let auth = users_auth(file);
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer alice-token".parse().unwrap(),
        );
        let principal = auth.principal(&headers);
        assert_eq!(principal.name.as_deref(), Some("alice"));
        assert_eq!(principal.roles, vec!["analyst"]);
        assert!(!principal.admin);

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer secret".parse().unwrap(),
        );
        let principal = auth.principal(&headers);
        assert_eq!(principal.name.as_deref(), Some("hashed"));
    }

    #[test]
    fn admin_role_from_users_file() {
        let file = "douchdb-auth-test-admin-users.json";
        write_users(file);
        let auth = users_auth(file);
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer root-token".parse().unwrap(),
        );
        let principal = auth.principal(&headers);
        assert!(principal.admin);
        assert!(principal.has_role("_admin"));
    }

    fn load(contents: &str) -> Result<Auth, String> {
        let file =
            std::env::temp_dir().join(format!("douchdb-auth-load-{}.json", crate::util::uuid()));
        std::fs::write(&file, contents).unwrap();
        let result = Auth::from_users_file(
            Some(("admin".into(), "secret".into())),
            &file,
            b"session-secret".to_vec(),
        );
        let _ = std::fs::remove_file(&file);
        result
    }

    #[test]
    fn users_file_name_colliding_with_admin_is_rejected() {
        let error = load(r#"{"users":[{"name":"admin","token":"long-enough-token"}]}"#)
            .err()
            .expect("collision must be an error");
        assert!(error.contains("admin"), "{error}");
        // A different name is fine.
        assert!(
            load(r#"{"users":[{"name":"administrator","token":"long-enough-token"}]}"#).is_ok()
        );
    }

    #[test]
    fn empty_and_short_tokens_are_rejected_at_load() {
        for entry in [
            r#"{"name":"a","token":""}"#,
            r#"{"name":"a","token":"short"}"#,
            // SHA-256 of the empty string.
            r#"{"name":"a","token_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}"#,
        ] {
            let contents = format!(r#"{{"users":[{entry}]}}"#);
            assert!(load(&contents).is_err(), "accepted {entry}");
        }
        assert!(load(r#"{"users":[{"name":"a","token":"long-enough-token"}]}"#).is_ok());
    }

    #[test]
    fn non_ascii_token_digest_is_an_error_not_a_panic() {
        // 64 bytes, but 'é' is two bytes, so a byte slice would split it.
        let digest = format!("a{}", "é".repeat(31) + "b");
        assert_eq!(digest.len(), 64);
        let contents = format!(r#"{{"users":[{{"name":"a","token_sha256":"{digest}"}}]}}"#);
        assert!(load(&contents).is_err());
    }

    #[test]
    fn empty_credentials_never_authenticate_even_if_an_entry_exists() {
        let mut auth = Auth::new("admin".into(), String::new(), b"k".to_vec());
        auth.users.push(User {
            name: "empty".into(),
            token: Some(String::new()),
            token_sha256: Some(Sha256::digest(b"").into()),
            roles: vec![],
            admin: false,
            salt: [0; 32],
        });
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer ".parse().unwrap(),
        );
        assert!(!auth.principal(&headers).is_authenticated());
        let basic = |text: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::AUTHORIZATION,
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(text)
                )
                .parse()
                .unwrap(),
            );
            headers
        };
        for text in ["empty:", "admin:", ":", ""] {
            assert!(
                !auth.principal(&basic(text)).is_authenticated(),
                "{text:?} authenticated"
            );
        }
        assert!(auth.authenticate("empty", "").is_none());
        assert!(auth.authenticate("admin", "").is_none());
        assert!(auth.authenticate("", "").is_none());
        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::COOKIE, "AuthSession=".parse().unwrap());
        assert!(!auth.principal(&headers).is_authenticated());
    }
}

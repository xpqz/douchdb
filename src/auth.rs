//! Minimal auth: HTTP basic, plus a stateless HMAC cookie so that any node in
//! the cluster can validate a session without shared storage.

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

const COOKIE: &str = "AuthSession";
const TTL_SECS: u64 = 60 * 60 * 24;

pub struct Auth {
    pub user: String,
    pub pass: String,
    secret: Vec<u8>,
}

impl Auth {
    pub fn new(user: String, pass: String) -> Auth {
        let secret = format!("{user}:{pass}:douchdb").into_bytes();
        Auth { user, pass, secret }
    }

    pub fn check_basic(&self, header: &str) -> bool {
        let Some(encoded) = header.strip_prefix("Basic ") else {
            return false;
        };
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded.trim()) else {
            return false;
        };
        let Ok(text) = String::from_utf8(decoded) else {
            return false;
        };
        match text.split_once(':') {
            Some((user, pass)) => {
                constant_time_eq(user.as_bytes(), self.user.as_bytes())
                    && constant_time_eq(pass.as_bytes(), self.pass.as_bytes())
            }
            None => false,
        }
    }

    fn sign(&self, payload: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret).expect("hmac key");
        mac.update(payload.as_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    }

    pub fn cookie_header(&self) -> String {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + TTL_SECS;
        let payload = format!("{}:{exp}", self.user);
        let token = format!(
            "{}.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.as_bytes()),
            self.sign(&payload)
        );
        format!("{COOKIE}={token}; Version=1; Path=/; HttpOnly; SameSite=Strict")
    }

    pub fn check_cookies(&self, header: Option<&str>) -> bool {
        let Some(header) = header else { return false };
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
            let Ok(payload) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload_b64)
            else {
                continue;
            };
            let Ok(payload) = String::from_utf8(payload) else {
                continue;
            };
            let Ok(signature) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(sig) else {
                continue;
            };
            let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret).expect("hmac key");
            mac.update(payload.as_bytes());
            if mac.verify_slice(&signature).is_err() {
                continue;
            }
            let Some((user, exp)) = payload.rsplit_once(':') else {
                continue;
            };
            let Ok(exp) = exp.parse::<u64>() else {
                continue;
            };
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if user == self.user && exp > now {
                return true;
            }
        }
        false
    }
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

    #[test]
    fn basic_auth() {
        let auth = Auth::new("admin".into(), "secret".into());
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
        let auth = Auth::new("admin".into(), "secret".into());
        let header = auth.cookie_header();
        assert!(header.contains("SameSite=Strict"));
        let value = header.split(';').next().unwrap();
        assert!(auth.check_cookies(Some(value)));
        assert!(!auth.check_cookies(Some("AuthSession=forged")));
        assert!(!auth.check_cookies(None));
    }
}

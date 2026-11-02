use super::{ApiError, OidcConfig};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, jwk::JwkSet};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::OpenOptions,
    io::{self, Read},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    Admin,
}
#[derive(Clone, serde::Serialize)]
pub struct Principal {
    pub name: String,
    pub role: Role,
    pub can_reveal: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenFile {
    tokens: Vec<Token>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Token {
    name: String,
    role: Role,
    sha256: String,
}
struct HashedToken {
    name: String,
    role: Role,
    hash: [u8; 32],
}
pub struct Auth {
    tokens: Vec<HashedToken>,
    oidc: Option<Oidc>,
}
struct Oidc {
    config: OidcConfig,
    client: reqwest::Client,
    cache: Mutex<Option<(Instant, Arc<JwkSet>)>>,
    last_fetch: Mutex<Option<Instant>>,
}
#[derive(Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    roles: Vec<String>,
}
impl Auth {
    pub fn load(path: &Path, config: Option<OidcConfig>) -> io::Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.len() > 65536
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err(invalid("invalid or writable management token policy"));
        }
        let mut data = Vec::new();
        file.by_ref().take(65537).read_to_end(&mut data)?;
        if data.len() > 65536 {
            return Err(invalid("oversized token policy"));
        }
        let policy: TokenFile = serde_json::from_slice(&data)
            .map_err(|_| invalid("invalid management token policy"))?;
        if policy.tokens.len() > 128 || (policy.tokens.is_empty() && config.is_none()) {
            return Err(invalid("invalid token count"));
        }
        let mut names = HashSet::new();
        let mut hashes = HashSet::new();
        let mut tokens = Vec::new();
        for token in policy.tokens {
            if token.name.is_empty()
                || token.name.len() > 128
                || token.name.chars().any(char::is_control)
                || !names.insert(token.name.clone())
                || token.sha256.len() != 64
            {
                return Err(invalid("invalid or duplicate token policy"));
            }
            let mut hash = [0; 32];
            for (i, pair) in token
                .sha256
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .enumerate()
            {
                let s = std::str::from_utf8(pair).map_err(|_| invalid("invalid token hash"))?;
                hash[i] = u8::from_str_radix(s, 16).map_err(|_| invalid("invalid token hash"))?;
            }
            if !hashes.insert(hash) {
                return Err(invalid("duplicate token hash"));
            }
            tokens.push(HashedToken {
                name: token.name,
                role: token.role,
                hash,
            });
        }
        let oidc = if let Some(config) = config {
            validate_url(&config.jwks_url, config.allow_loopback_http)?;
            validate_url(&config.issuer, config.allow_loopback_http)?;
            if config.audience.is_empty()
                || config.audience.len() > 512
                || config.admin_role.is_empty()
                || config.viewer_role.is_empty()
                || config.admin_role == config.viewer_role
            {
                return Err(invalid("invalid OIDC policy"));
            }
            Some(Oidc {
                config,
                client: reqwest::Client::builder()
                    .timeout(Duration::from_secs(5))
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .map_err(|_| invalid("OIDC client initialization failed"))?,
                cache: Mutex::new(None),
                last_fetch: Mutex::new(None),
            })
        } else {
            None
        };
        Ok(Self { tokens, oidc })
    }
    pub async fn authenticate(&self, token: &str) -> Result<Principal, ApiError> {
        if token.is_empty() || token.len() > 16384 || token.chars().any(char::is_whitespace) {
            return Err(ApiError::unauthorized());
        }
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut matched = None;
        for item in &self.tokens {
            if bool::from(item.hash.ct_eq(&digest)) {
                matched = Some(item);
            }
        }
        if let Some(item) = matched {
            return Ok(Principal {
                name: item.name.clone(),
                role: item.role,
                can_reveal: item.role == Role::Admin,
            });
        }
        if let Some(oidc) = &self.oidc {
            return oidc.authenticate(token).await;
        }
        Err(ApiError::unauthorized())
    }
}
impl Oidc {
    async fn keys(&self) -> Result<Arc<JwkSet>, ApiError> {
        let mut cache = self.cache.lock().await;
        if let Some((at, keys)) = &*cache
            && at.elapsed() < Duration::from_secs(300)
        {
            return Ok(keys.clone());
        }
        let mut last = self.last_fetch.lock().await;
        if last.is_some_and(|at| at.elapsed() < Duration::from_secs(10)) {
            return Err(ApiError::unauthorized());
        }
        *last = Some(Instant::now());
        let mut response = self
            .client
            .get(&self.config.jwks_url)
            .send()
            .await
            .map_err(|_| ApiError::unauthorized())?;
        if !response.status().is_success() || response.content_length().is_some_and(|n| n > 262144)
        {
            return Err(ApiError::unauthorized());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ApiError::unauthorized())?
        {
            if bytes.len() + chunk.len() > 262144 {
                return Err(ApiError::unauthorized());
            }
            bytes.extend_from_slice(&chunk);
        }
        let keys: JwkSet = serde_json::from_slice(&bytes).map_err(|_| ApiError::unauthorized())?;
        if keys.keys.is_empty() || keys.keys.len() > 32 {
            return Err(ApiError::unauthorized());
        }
        let mut ids = HashSet::new();
        for key in &keys.keys {
            if let Some(id) = &key.common.key_id
                && (id.is_empty() || id.len() > 256 || !ids.insert(id))
            {
                return Err(ApiError::unauthorized());
            }
        }
        let keys = Arc::new(keys);
        *cache = Some((Instant::now(), keys.clone()));
        Ok(keys)
    }
    async fn authenticate(&self, token: &str) -> Result<Principal, ApiError> {
        use jsonwebtoken::jwk::{KeyAlgorithm, KeyOperations, PublicKeyUse};
        let header = jsonwebtoken::decode_header(token).map_err(|_| ApiError::unauthorized())?;
        if !matches!(header.alg, Algorithm::RS256 | Algorithm::ES256) {
            return Err(ApiError::unauthorized());
        }
        let kid = header
            .kid
            .filter(|s| !s.is_empty() && s.len() <= 256)
            .ok_or_else(ApiError::unauthorized)?;
        let keys = self.keys().await?;
        let jwk = keys.find(&kid).ok_or_else(ApiError::unauthorized)?;
        let expected = match header.alg {
            Algorithm::RS256 => KeyAlgorithm::RS256,
            _ => KeyAlgorithm::ES256,
        };
        if jwk.common.key_algorithm.is_some_and(|alg| alg != expected)
            || jwk
                .common
                .public_key_use
                .as_ref()
                .is_some_and(|usage| *usage != PublicKeyUse::Signature)
            || jwk
                .common
                .key_operations
                .as_ref()
                .is_some_and(|ops| !ops.contains(&KeyOperations::Verify))
        {
            return Err(ApiError::unauthorized());
        }
        let key = DecodingKey::from_jwk(jwk).map_err(|_| ApiError::unauthorized())?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        let claims = jsonwebtoken::decode::<Claims>(token, &key, &validation)
            .map_err(|_| ApiError::unauthorized())?
            .claims;
        if claims.sub.is_empty()
            || claims.sub.len() > 256
            || claims.sub.chars().any(char::is_control)
            || claims.roles.len() > 128
        {
            return Err(ApiError::unauthorized());
        }
        let role = if claims.roles.contains(&self.config.admin_role) {
            Role::Admin
        } else if claims.roles.contains(&self.config.viewer_role) {
            Role::Viewer
        } else {
            return Err(ApiError::forbidden());
        };
        Ok(Principal {
            name: claims.sub,
            role,
            can_reveal: role == Role::Admin,
        })
    }
}
fn validate_url(value: &str, allow: bool) -> io::Result<()> {
    let url = reqwest::Url::parse(value).map_err(|_| invalid("invalid OIDC URL"))?;
    let loopback = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
    );
    if !(url.scheme() == "https" || (allow && loopback && url.scheme() == "http"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid("OIDC requires HTTPS"));
    }
    Ok(())
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    fn policy(value: serde_json::Value) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ffkv-auth-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        file.write_all(&serde_json::to_vec(&value).unwrap())
            .unwrap();
        path
    }
    #[tokio::test]
    async fn hashes_authenticate_and_wrong_tokens_fail() {
        let digest = Sha256::digest(b"a-long-random-test-token");
        let hash = digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let path = policy(
            serde_json::json!({"tokens":[{"name":"observer","role":"viewer","sha256":hash}]}),
        );
        let auth = Auth::load(&path, None).unwrap();
        let principal = auth.authenticate("a-long-random-test-token").await.unwrap();
        assert_eq!(principal.role, Role::Viewer);
        assert!(!principal.can_reveal);
        assert!(auth.authenticate("incorrect").await.is_err());
        assert!(auth.authenticate(&"x".repeat(16385)).await.is_err());
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn rejects_duplicate_policy_and_symlinks() {
        let token = serde_json::json!({"name":"operator","role":"admin","sha256":"00".repeat(32)});
        let path = policy(serde_json::json!({"tokens":[token.clone(),token]}));
        assert!(Auth::load(&path, None).is_err());
        let link = path.with_extension("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(Auth::load(&link, None).is_err());
        std::fs::remove_file(link).unwrap();
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn oidc_transport_is_fail_closed() {
        assert!(validate_url("http://issuer.example/jwks", true).is_err());
        assert!(validate_url("http://127.0.0.1/jwks", false).is_err());
        assert!(validate_url("http://127.0.0.1/jwks", true).is_ok());
        assert!(validate_url("https://user:password@example.com/jwks", false).is_err());
    }
}

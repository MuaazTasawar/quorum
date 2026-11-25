//! Authentication for the HTTP API: bearer tokens, each with a role.
//!
//! A request carries `Authorization: Bearer <token>`. The token decides which of three roles the
//! caller has, and each route needs a minimum role:
//!
//! | role    | may do                                            |
//! |---------|---------------------------------------------------|
//! | `read`  | read keys                                         |
//! | `write` | everything `read` may, and write and delete keys  |
//! | `admin` | everything `write` may, and change the membership |
//!
//! With no tokens configured, authentication is off and the API behaves as it always has. With
//! tokens configured, `/health`, `/metrics`, `/ws` and `/dashboard` stay open by default (a
//! Prometheus scrape or the dashboard has no token to send); `protect_observability` puts the
//! first three behind the `read` role as well. `/dashboard` itself is a static page with no data
//! in it, so it is always open.
//!
//! Choices that matter for safety:
//!
//! * **Fail closed.** A route this module does not know about needs `admin`, so a route added
//!   later is protected until someone decides otherwise. A token file that cannot be read, or
//!   that holds no tokens, stops the node from starting instead of silently turning
//!   authentication off.
//! * **No secrets in output.** Tokens are never logged, never shown by `Debug`, and never
//!   repeated in an error message (including for a line whose words are the wrong way round).
//! * **Constant-time comparison.** A presented token is compared with every configured token,
//!   without stopping early at the first difference or the first match.
//! * **The same answer for "no token" and "wrong token"**, so a caller cannot learn anything
//!   from the difference.
//!
//! A bearer token is a password that travels in every request. Until the API speaks TLS, anyone
//! who can watch the network can read it.

use axum::http::{header, HeaderMap, Method};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

/// The shortest token accepted. 16 characters is the floor, not a recommendation: generate
/// 32 random bytes and write them as hex (64 characters).
pub const MIN_TOKEN_LEN: usize = 16;
/// The longest token accepted (a header has to carry it).
pub const MAX_TOKEN_LEN: usize = 256;
/// The most tokens a file may hold.
pub const MAX_TOKENS: usize = 1000;

/// What a caller may do. Ordered: each role may do everything the one before it may.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    Read,
    Write,
    Admin,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Role::Read => "read",
            Role::Write => "write",
            Role::Admin => "admin",
        }
    }

    fn parse(word: &str) -> Option<Role> {
        match word.to_ascii_lowercase().as_str() {
            "read" => Some(Role::Read),
            "write" => Some(Role::Write),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }
}

/// What to do with one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    /// No usable token, or one that matches nothing: 401.
    Unauthorized,
    /// A valid token whose role is too low for this route: 403.
    Forbidden {
        needed: Role,
    },
}

struct Token {
    role: Role,
    secret: Vec<u8>,
}

/// The API's access rules. `Auth::default()` is "off": every request is allowed.
#[derive(Default)]
pub struct Auth {
    tokens: Vec<Token>,
    protect_observability: bool,
}

// Deliberately not derived: a derived Debug would print every token.
impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [read, write, admin] = self.role_counts();
        f.debug_struct("Auth")
            .field("enabled", &self.enabled())
            .field(
                "tokens",
                &format_args!("{read} read, {write} write, {admin} admin"),
            )
            .field("protect_observability", &self.protect_observability)
            .finish()
    }
}

impl Auth {
    /// Authentication off: everything is allowed.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Builds the rules from `(role, token)` pairs. The same checks as a token file: each token
    /// within the length limits and printable, none repeated, at least one given.
    pub fn from_entries(entries: Vec<(Role, String)>) -> anyhow::Result<Self> {
        let numbered = entries
            .into_iter()
            .enumerate()
            .map(|(i, (role, secret))| (i + 1, role, secret))
            .collect();
        Self::build(numbered)
    }

    /// Reads a token file: one `<role> <token>` per line; blank lines and lines starting with
    /// `#` are ignored. See the module docs for the roles.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        // Windows PowerShell writes UTF-8 files with a byte order mark.
        let text = text.strip_prefix('\u{feff}').unwrap_or(text);
        let mut entries = Vec::new();
        for (index, raw) in text.lines().enumerate() {
            let line = index + 1;
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let mut words = trimmed.split_whitespace();
            let (Some(role_word), Some(secret), None) = (words.next(), words.next(), words.next())
            else {
                // Nothing of the line is quoted back: either word could be the token.
                anyhow::bail!("token file, line {line}: expected `<role> <token>`");
            };
            let Some(role) = Role::parse(role_word) else {
                anyhow::bail!(
                    "token file, line {line}: the first word must be read, write or admin \
                     (the role comes before the token)"
                );
            };
            entries.push((line, role, secret.to_string()));
        }
        Self::build(entries)
    }

    /// Reads and parses the token file at `path`.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read the token file {}: {e}", path.display()))?;
        warn_if_readable_by_others(path);
        Self::parse(&text)
            .map_err(|e| anyhow::anyhow!("the token file {} is not usable: {e}", path.display()))
    }

    fn build(entries: Vec<(usize, Role, String)>) -> anyhow::Result<Self> {
        if entries.is_empty() {
            anyhow::bail!(
                "it holds no tokens; refusing to start with authentication silently off \
                 (remove QUORUM_TOKENS_FILE to run without authentication)"
            );
        }
        if entries.len() > MAX_TOKENS {
            anyhow::bail!("it holds more than {MAX_TOKENS} tokens");
        }
        let mut seen: HashMap<&str, usize> = HashMap::new();
        for (line, _, secret) in &entries {
            if secret.len() < MIN_TOKEN_LEN {
                anyhow::bail!(
                    "line {line}: a token must be at least {MIN_TOKEN_LEN} characters \
                     (use 32 random bytes as hex)"
                );
            }
            if secret.len() > MAX_TOKEN_LEN {
                anyhow::bail!("line {line}: a token must be at most {MAX_TOKEN_LEN} characters");
            }
            if !secret.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
                anyhow::bail!("line {line}: a token may only use printable ASCII, no spaces");
            }
            if let Some(first) = seen.insert(secret.as_str(), *line) {
                anyhow::bail!(
                    "lines {first} and {line} hold the same token; every token must be unique"
                );
            }
        }
        let tokens = entries
            .into_iter()
            .map(|(_, role, secret)| Token {
                role,
                secret: secret.into_bytes(),
            })
            .collect();
        Ok(Self {
            tokens,
            protect_observability: false,
        })
    }

    /// Also requires the `read` role for `/health`, `/metrics` and `/ws`.
    pub fn with_protected_observability(mut self, protect: bool) -> Self {
        self.protect_observability = protect;
        self
    }

    pub fn enabled(&self) -> bool {
        !self.tokens.is_empty()
    }

    pub fn protects_observability(&self) -> bool {
        self.protect_observability
    }

    /// How many tokens each role has: `[read, write, admin]`.
    pub fn role_counts(&self) -> [usize; 3] {
        let mut counts = [0; 3];
        for token in &self.tokens {
            counts[token.role as usize] += 1;
        }
        counts
    }

    /// The lowest role that may use a route, or `None` if anyone may. `route` is the route
    /// template (`/kv/{key}`), or `unmatched` for a path no route claims.
    pub fn required_role(&self, route: &str, method: &Method) -> Option<Role> {
        match route {
            // Nothing sensitive: a 404, and the static dashboard page.
            "unmatched" | "/dashboard" => None,
            "/health" | "/metrics" | "/ws" => self.protect_observability.then_some(Role::Read),
            "/kv/{key}" => Some(if method == Method::GET || method == Method::HEAD {
                Role::Read
            } else {
                Role::Write
            }),
            // `/admin/...`, and anything added later that nobody has classified.
            _ => Some(Role::Admin),
        }
    }

    /// Decides one request. `token` is what the caller presented, already taken out of the
    /// header (see [`bearer_token`]).
    pub fn check(&self, route: &str, method: &Method, token: Option<&[u8]>) -> Decision {
        if !self.enabled() {
            return Decision::Allow;
        }
        let Some(needed) = self.required_role(route, method) else {
            return Decision::Allow;
        };
        let Some(presented) = token else {
            return Decision::Unauthorized;
        };
        match self.role_of(presented) {
            None => Decision::Unauthorized,
            Some(role) if role >= needed => Decision::Allow,
            Some(_) => Decision::Forbidden { needed },
        }
    }

    /// The role of the token, comparing against every configured token with no early exit.
    fn role_of(&self, presented: &[u8]) -> Option<Role> {
        let mut found = None;
        for token in &self.tokens {
            if constant_time_eq(&token.secret, presented) {
                found = Some(token.role);
            }
        }
        found
    }
}

/// Equality that takes the same time wherever the inputs differ. The length is included in the
/// comparison, not tested first, and the loop always runs to the longer of the two.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

/// The token from an `Authorization: Bearer <token>` header, or `None` if there is no header,
/// more than one (which one counts?), it is not a bearer credential, or the token is empty or
/// holds whitespace. The scheme name is case-insensitive (RFC 9110).
pub fn bearer_token(headers: &HeaderMap) -> Option<&[u8]> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let value = value.as_bytes();
    let split = value.iter().position(|&b| b == b' ')?;
    let (scheme, rest) = value.split_at(split);
    if !scheme.eq_ignore_ascii_case(b"bearer") {
        return None;
    }
    let token = {
        let start = rest.iter().position(|&b| b != b' ')?;
        &rest[start..]
    };
    if token.is_empty() || token.iter().any(|b| b.is_ascii_whitespace()) {
        return None;
    }
    Some(token)
}

/// A token file that other users can read is a leaked token file. Only checked where the
/// permission bits mean something; a warning, because the node's owner may know better.
fn warn_if_readable_by_others(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            if meta.permissions().mode() & 0o077 != 0 {
                tracing::warn!(
                    path = %path.display(),
                    "the token file can be read by other users; restrict it (chmod 600)"
                );
            }
        }
    }
    #[cfg(not(unix))]
    let _ = path;
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    const READ: &str = "read-token-0123456789abcdef";
    const WRITE: &str = "write-token-0123456789abcdef";
    const ADMIN: &str = "admin-token-0123456789abcdef";

    fn auth() -> Auth {
        Auth::from_entries(vec![
            (Role::Read, READ.into()),
            (Role::Write, WRITE.into()),
            (Role::Admin, ADMIN.into()),
        ])
        .unwrap()
    }

    fn check(auth: &Auth, route: &str, method: Method, token: Option<&str>) -> Decision {
        auth.check(route, &method, token.map(str::as_bytes))
    }

    fn headers(values: &[&str]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for value in values {
            map.append(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        }
        map
    }

    // ---- roles ----

    #[test]
    fn roles_are_ordered_read_below_write_below_admin() {
        assert!(Role::Read < Role::Write && Role::Write < Role::Admin);
        assert_eq!(Role::Admin.name(), "admin");
    }

    #[test]
    fn role_words_are_case_insensitive_and_nothing_else_is_a_role() {
        assert_eq!(Role::parse("ADMIN"), Some(Role::Admin));
        assert_eq!(Role::parse("Read"), Some(Role::Read));
        assert_eq!(Role::parse("root"), None);
        assert_eq!(Role::parse(""), None);
    }

    // ---- which role each route needs ----

    #[test]
    fn each_route_needs_the_role_its_effect_calls_for() {
        let a = auth();
        let need = |route, method| a.required_role(route, &method);
        assert_eq!(need("/kv/{key}", Method::GET), Some(Role::Read));
        assert_eq!(need("/kv/{key}", Method::HEAD), Some(Role::Read));
        assert_eq!(need("/kv/{key}", Method::PUT), Some(Role::Write));
        assert_eq!(need("/kv/{key}", Method::DELETE), Some(Role::Write));
        // Any other method on a key is treated as a change, not a read.
        assert_eq!(need("/kv/{key}", Method::POST), Some(Role::Write));
        assert_eq!(need("/kv/{key}", Method::PATCH), Some(Role::Write));
        assert_eq!(need("/admin/members", Method::POST), Some(Role::Admin));
        assert_eq!(
            need("/admin/members/{id}", Method::DELETE),
            Some(Role::Admin)
        );
        assert_eq!(need("/dashboard", Method::GET), None);
        assert_eq!(need("unmatched", Method::GET), None);
    }

    #[test]
    fn a_route_nobody_classified_needs_admin() {
        let a = auth();
        assert_eq!(
            a.required_role("/some/future/route", &Method::GET),
            Some(Role::Admin)
        );
        assert_eq!(
            a.required_role("/admin/something-new", &Method::GET),
            Some(Role::Admin)
        );
    }

    #[test]
    fn observability_is_open_unless_protected() {
        let open = auth();
        let closed = auth().with_protected_observability(true);
        for route in ["/health", "/metrics", "/ws"] {
            assert_eq!(open.required_role(route, &Method::GET), None, "{route}");
            assert_eq!(
                closed.required_role(route, &Method::GET),
                Some(Role::Read),
                "{route}"
            );
        }
        // The page itself holds no data and stays open either way.
        assert_eq!(closed.required_role("/dashboard", &Method::GET), None);
    }

    // ---- deciding a request ----

    #[test]
    fn with_no_tokens_configured_everything_is_allowed() {
        let off = Auth::disabled();
        assert!(!off.enabled());
        for route in ["/kv/{key}", "/admin/members", "/health", "/other"] {
            assert_eq!(check(&off, route, Method::PUT, None), Decision::Allow);
        }
        // Even asking for protection changes nothing without tokens.
        let off = Auth::disabled().with_protected_observability(true);
        assert_eq!(check(&off, "/health", Method::GET, None), Decision::Allow);
    }

    #[test]
    fn a_missing_or_wrong_token_is_unauthorized() {
        let a = auth();
        assert_eq!(
            check(&a, "/kv/{key}", Method::GET, None),
            Decision::Unauthorized
        );
        assert_eq!(
            check(&a, "/kv/{key}", Method::GET, Some("nope")),
            Decision::Unauthorized
        );
        assert_eq!(
            check(&a, "/kv/{key}", Method::GET, Some("")),
            Decision::Unauthorized
        );
        // Nearly right is still wrong: a prefix, an extension, a changed last character.
        for almost in [
            &READ[..READ.len() - 1],
            &format!("{READ}x"),
            &format!("{}X", &READ[..READ.len() - 1]),
        ] {
            assert_eq!(
                check(&a, "/kv/{key}", Method::GET, Some(almost)),
                Decision::Unauthorized,
                "{almost}"
            );
        }
    }

    #[test]
    fn a_token_works_exactly_up_to_its_role() {
        let a = auth();
        let get = |t| check(&a, "/kv/{key}", Method::GET, Some(t));
        let put = |t| check(&a, "/kv/{key}", Method::PUT, Some(t));
        let admin = |t| check(&a, "/admin/members", Method::POST, Some(t));
        assert_eq!(
            (get(READ), get(WRITE), get(ADMIN)),
            (Decision::Allow, Decision::Allow, Decision::Allow)
        );
        assert_eq!(
            put(READ),
            Decision::Forbidden {
                needed: Role::Write
            }
        );
        assert_eq!((put(WRITE), put(ADMIN)), (Decision::Allow, Decision::Allow));
        assert_eq!(
            admin(READ),
            Decision::Forbidden {
                needed: Role::Admin
            }
        );
        assert_eq!(
            admin(WRITE),
            Decision::Forbidden {
                needed: Role::Admin
            }
        );
        assert_eq!(admin(ADMIN), Decision::Allow);
    }

    #[test]
    fn protected_observability_asks_for_a_token() {
        let a = auth().with_protected_observability(true);
        assert_eq!(
            check(&a, "/health", Method::GET, None),
            Decision::Unauthorized
        );
        assert_eq!(
            check(&a, "/metrics", Method::GET, Some(READ)),
            Decision::Allow
        );
        assert_eq!(check(&a, "/dashboard", Method::GET, None), Decision::Allow);
    }

    // ---- the header ----

    #[test]
    fn the_bearer_header_is_read_strictly() {
        let token = |values: &[&str]| {
            bearer_token(&headers(values)).map(|t| String::from_utf8(t.to_vec()).unwrap())
        };
        assert_eq!(token(&["Bearer abc123"]).as_deref(), Some("abc123"));
        // The scheme name is case-insensitive; extra spaces before the token are tolerated.
        assert_eq!(token(&["bearer abc123"]).as_deref(), Some("abc123"));
        assert_eq!(token(&["BEARER   abc123"]).as_deref(), Some("abc123"));
        // Everything else is no credential.
        assert_eq!(token(&[]), None);
        assert_eq!(token(&["Bearer"]), None);
        assert_eq!(token(&["Bearer "]), None);
        assert_eq!(token(&["Bearer a b"]), None);
        assert_eq!(token(&["Basic dXNlcjpwYXNz"]), None);
        assert_eq!(token(&["abc123"]), None);
        assert_eq!(token(&["Bearerabc123"]), None);
        // Two Authorization headers: which one counts? Neither.
        assert_eq!(token(&["Bearer abc123", "Bearer def456"]), None);
        assert_eq!(token(&["Bearer abc123", "Bearer abc123"]), None);
    }

    // ---- comparing tokens ----

    #[test]
    fn constant_time_eq_agrees_with_ordinary_equality() {
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"abcd", b"abc"));
        assert!(!constant_time_eq(b"abc", b""));
        // A trailing NUL must not be mistaken for the padding.
        assert!(!constant_time_eq(b"abc", b"abc\0"));
        assert!(!constant_time_eq(b"abc\0", b"abc"));
        for i in 0..8 {
            let mut other = *b"01234567";
            other[i] ^= 1;
            assert!(!constant_time_eq(b"01234567", &other), "differs at {i}");
        }
    }

    // ---- the token file ----

    #[test]
    fn a_token_file_is_parsed_line_by_line() {
        let text = format!(
            "\u{feff}# who may do what\r\n\r\nREAD   {READ}\r\n  write {WRITE}  \r\nadmin {ADMIN} \r\n"
        );
        let a = Auth::parse(&text).unwrap();
        assert_eq!(a.role_counts(), [1, 1, 1]);
        assert_eq!(
            check(&a, "/kv/{key}", Method::PUT, Some(WRITE)),
            Decision::Allow
        );
        assert_eq!(
            check(&a, "/kv/{key}", Method::PUT, Some(READ)),
            Decision::Forbidden {
                needed: Role::Write
            }
        );
    }

    #[test]
    fn a_file_without_tokens_is_refused_not_treated_as_authentication_off() {
        for text in ["", "\n\n", "# only comments\n  # and blanks\n"] {
            let error = Auth::parse(text).unwrap_err().to_string();
            assert!(error.contains("no tokens"), "{error}");
        }
    }

    #[test]
    fn bad_lines_are_refused_with_a_line_number_and_never_quote_the_line() {
        let long = "x".repeat(MAX_TOKEN_LEN + 1);
        let cases: Vec<(String, &str)> = vec![
            (format!("read {READ} extra"), "line 1"),
            ("read".to_string(), "line 1"),
            (READ.to_string(), "line 1"),
            (format!("root {READ}"), "line 1"),
            // The wrong way round: the "role" word is really the token.
            (format!("{READ} read"), "line 1"),
            ("read short".to_string(), "at least"),
            (format!("read {long}"), "at most"),
            ("read abcdefghijklmnop\u{e9}".to_string(), "printable ASCII"),
            (format!("# fine\nwrite {WRITE}\nread tooshort"), "line 3"),
        ];
        for (text, expected) in cases {
            let error = Auth::parse(&text).unwrap_err().to_string();
            assert!(error.contains(expected), "{text:?} -> {error}");
            for secret in [READ, WRITE, "abcdefghijklmnop", "short", "tooshort"] {
                assert!(!error.contains(secret), "{error} quotes a secret");
            }
        }
    }

    #[test]
    fn a_repeated_token_is_refused_whatever_the_roles() {
        let text = format!("read {READ}\nadmin {READ}\n");
        let error = Auth::parse(&text).unwrap_err().to_string();
        assert!(error.contains("lines 1 and 2"), "{error}");
        assert!(!error.contains(READ));
    }

    #[test]
    fn the_token_count_is_limited() {
        let entries = (0..=MAX_TOKENS)
            .map(|i| (Role::Read, format!("token-number-{i:08}-padding")))
            .collect();
        assert!(Auth::from_entries(entries).is_err());
    }

    #[test]
    fn nothing_shows_the_tokens_in_debug_output() {
        let a = auth().with_protected_observability(true);
        let shown = format!("{a:?}");
        for secret in [READ, WRITE, ADMIN] {
            assert!(!shown.contains(secret), "{shown}");
        }
        assert!(shown.contains("1 read, 1 write, 1 admin"), "{shown}");
    }

    #[test]
    fn a_missing_token_file_stops_the_node_naming_the_path_only() {
        let error = Auth::load(Path::new("/definitely/not/here/tokens.txt"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("/definitely/not/here/tokens.txt"), "{error}");
    }

    #[test]
    fn a_file_on_disk_loads_through_the_same_checks() {
        let dir = std::env::temp_dir().join(format!("quorum-auth-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tokens.txt");
        std::fs::write(&path, format!("admin {ADMIN}\nread {READ}\n")).unwrap();
        let a = Auth::load(&path).unwrap();
        assert_eq!(a.role_counts(), [1, 0, 1]);
        std::fs::write(&path, "read short\n").unwrap();
        let error = Auth::load(&path).unwrap_err().to_string();
        assert!(
            error.contains("not usable") && error.contains("at least"),
            "{error}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

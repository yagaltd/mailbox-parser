use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EmailAddress {
    pub address: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl EmailAddress {
    pub fn new(address: impl AsRef<str>, name: Option<String>) -> Option<Self> {
        let address = normalize_email_address(address.as_ref())?;
        let name = name.and_then(|n| {
            let t = n.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        });
        Some(Self { address, name })
    }

    pub fn parse(input: &str) -> Option<Self> {
        let s = input.trim();
        if s.is_empty() {
            return None;
        }

        if let (Some(start), Some(end)) = (s.find('<'), s.rfind('>'))
            && start < end
        {
            let name = s[..start].trim().trim_matches('"').to_string();
            let addr = s[start + 1..end].trim();
            return Self::new(addr, if name.is_empty() { None } else { Some(name) });
        }

        let cleaned = s.trim_matches('"');
        Self::new(cleaned, None)
    }
}

pub fn normalize_email_address(s: &str) -> Option<String> {
    let s = s.trim();
    let s = if let (Some(start), Some(end)) = (s.find('<'), s.rfind('>')) {
        if start < end {
            s[start + 1..end].trim()
        } else {
            s
        }
    } else {
        s
    };
    let s = s.trim_matches(|c: char| c == '<' || c == '>' || c == ',' || c == '.' || c == ';');
    let s = s.to_lowercase();
    if s.len() < 6 || s.len() > 254 {
        return None;
    }
    let at = s.find('@')?;
    if at == 0 || at + 1 >= s.len() {
        return None;
    }
    if !s[at + 1..].contains('.') {
        return None;
    }
    Some(s)
}

pub fn entity_id_for_email(s: &str) -> Option<String> {
    let normalized = normalize_email_address(s)?;
    Some(entity_id_for_normalized_email(&normalized))
}

pub fn entity_id_for_normalized_email(normalized: &str) -> String {
    format!("email:{}", sha256_hex(normalized.as_bytes()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(bytes);
    let out = h.finalize();

    let mut s = String::with_capacity(out.len() * 2);
    for b in out {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

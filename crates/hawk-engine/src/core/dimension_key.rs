use std::collections::BTreeMap;

pub type DimensionKey = BTreeMap<String, String>;

pub fn dimension_key_from_pairs<I, K, V>(pairs: I) -> DimensionKey
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    pairs
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect()
}

pub fn canonical_dimension_key(key: &DimensionKey) -> String {
    key.iter()
        .map(|(k, v)| format!("{}:{}", encode_component(k), encode_component(v)))
        .collect::<Vec<_>>()
        .join("/")
}

pub fn parse_canonical_dimension_key(input: &str) -> Option<DimensionKey> {
    if input.is_empty() {
        return Some(DimensionKey::new());
    }

    let mut key = DimensionKey::new();
    for part in input.split('/') {
        let (name, value) = part.split_once(':')?;
        key.insert(decode_component(name)?, decode_component(value)?);
    }
    Some(key)
}

fn encode_component(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        match byte {
            b'%' | b':' | b'/' => out.push_str(&format!("%{byte:02X}")),
            _ => out.push(byte as char),
        }
    }
    out
}

fn decode_component(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = *bytes.get(i + 1)?;
            let lo = *bytes.get(i + 2)?;
            out.push(hex_value(hi)? * 16 + hex_value(lo)?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

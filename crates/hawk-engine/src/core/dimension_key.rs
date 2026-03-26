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
        .map(|(k, v)| format!("{k}:{v}"))
        .collect::<Vec<_>>()
        .join("/")
}

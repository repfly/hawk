use std::collections::HashMap;

use anyhow::{anyhow, Result};

#[derive(Debug, Clone, Default)]
pub struct QueryReference {
    pub dimensions: HashMap<String, String>,
    pub variable: Option<String>,
}

pub fn parse_reference(input: &str) -> Result<QueryReference> {
    if input.trim().is_empty() {
        return Err(anyhow!("reference cannot be empty"));
    }

    let mut out = QueryReference::default();

    for part in input.split('/') {
        let Some((k, v)) = part.split_once(':') else {
            return Err(anyhow!("invalid reference segment '{}'", part));
        };
        if k == "variable" {
            out.variable = Some(v.to_owned());
        } else {
            out.dimensions.insert(k.to_owned(), v.to_owned());
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::parse_reference;

    #[test]
    fn parse_fully_qualified_reference() {
        let parsed = parse_reference("topic:russia-ukraine/time:2024-03/variable:leaning")
            .expect("parse should pass");
        assert_eq!(parsed.variable.as_deref(), Some("leaning"));
        assert_eq!(parsed.dimensions.get("topic").map(String::as_str), Some("russia-ukraine"));
        assert_eq!(parsed.dimensions.get("time").map(String::as_str), Some("2024-03"));
    }
}

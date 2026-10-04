#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Compare,
    Between,
    And,
    Explain,
    Vs,
    Track,
    From,
    Granularity,
    Show,
    At,
    Rank,
    By,
    Over,
    Mi,
    Cmi,
    Given,
    Correlations,
    Limit,
    Pairwise,
    On,
    Using,
    Nearest,
    Stats,
    Schema,
    Dimensions,
    Entropy,
    Where,
    Top,
    Bottom,
    Across,
    Export,
    As,
    Csv,
    Json,
    Alert,
    When,
    Surprise,
    Under,
    Structure,
    Estimate,
    Audit,
    Storage,
    Suggest,

    // Operators
    Gt,
    Lt,
    Gte,
    Lte,

    // Values
    Ident(String),
    DimRef(String, String), // dimension:value
    Number(usize),

    // Punctuation
    Comma,
    Semicolon,

    Eof,
}

pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let trimmed = input.trim().trim_end_matches(';');

    for word in trimmed.split_whitespace() {
        // Strip trailing commas/semicolons
        let (word, has_comma) = if let Some(stripped) = word.strip_suffix(',') {
            (stripped, true)
        } else {
            (word, false)
        };

        if word.is_empty() {
            if has_comma {
                tokens.push(Token::Comma);
            }
            continue;
        }

        let token = match word.to_ascii_uppercase().as_str() {
            "COMPARE" => Token::Compare,
            "BETWEEN" => Token::Between,
            "AND" => Token::And,
            "EXPLAIN" => Token::Explain,
            "VS" => Token::Vs,
            "TRACK" => Token::Track,
            "FROM" => Token::From,
            "GRANULARITY" => Token::Granularity,
            "SHOW" => Token::Show,
            "AT" => Token::At,
            "RANK" => Token::Rank,
            "BY" => Token::By,
            "OVER" => Token::Over,
            "MI" => Token::Mi,
            "CMI" => Token::Cmi,
            "GIVEN" => Token::Given,
            "CORRELATIONS" => Token::Correlations,
            "LIMIT" => Token::Limit,
            "PAIRWISE" => Token::Pairwise,
            "ON" => Token::On,
            "USING" => Token::Using,
            "NEAREST" => Token::Nearest,
            "STATS" => Token::Stats,
            "SCHEMA" => Token::Schema,
            "DIMENSIONS" => Token::Dimensions,
            "ENTROPY" => Token::Entropy,
            "WHERE" => Token::Where,
            "TOP" => Token::Top,
            "BOTTOM" => Token::Bottom,
            "ACROSS" => Token::Across,
            "EXPORT" => Token::Export,
            "AS" => Token::As,
            "CSV" => Token::Csv,
            "JSON" => Token::Json,
            "ALERT" => Token::Alert,
            "WHEN" => Token::When,
            "SURPRISE" => Token::Surprise,
            "UNDER" => Token::Under,
            "STRUCTURE" => Token::Structure,
            "ESTIMATE" => Token::Estimate,
            "AUDIT" => Token::Audit,
            "STORAGE" => Token::Storage,
            "SUGGEST" => Token::Suggest,
            ">=" => Token::Gte,
            "<=" => Token::Lte,
            ">" => Token::Gt,
            "<" => Token::Lt,
            _ => {
                // Try dimension:value
                if let Some(colon_pos) = word.find(':') {
                    let dim = &word[..colon_pos];
                    let val = &word[colon_pos + 1..];
                    if !dim.is_empty() && !val.is_empty() {
                        Token::DimRef(dim.to_owned(), val.to_owned())
                    } else {
                        return Err(format!("invalid dimension reference: '{}'", word));
                    }
                }
                // Try number
                else if let Ok(n) = word.parse::<usize>() {
                    Token::Number(n)
                }
                // Identifier
                else {
                    Token::Ident(word.to_owned())
                }
            }
        };

        tokens.push(token);
        if has_comma {
            tokens.push(Token::Comma);
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_compare() {
        let tokens = tokenize("COMPARE category BETWEEN time:2013 AND time:2022").unwrap();
        assert_eq!(tokens[0], Token::Compare);
        assert_eq!(tokens[1], Token::Ident("category".into()));
        assert_eq!(tokens[2], Token::Between);
        assert_eq!(tokens[3], Token::DimRef("time".into(), "2013".into()));
        assert_eq!(tokens[4], Token::And);
        assert_eq!(tokens[5], Token::DimRef("time".into(), "2022".into()));
    }

    #[test]
    fn tokenize_mi_with_commas() {
        let tokens = tokenize("MI author, category AT time:2022").unwrap();
        assert_eq!(tokens[0], Token::Mi);
        assert_eq!(tokens[1], Token::Ident("author".into()));
        assert_eq!(tokens[2], Token::Comma);
        assert_eq!(tokens[3], Token::Ident("category".into()));
    }

    #[test]
    fn tokenize_case_insensitive() {
        let tokens = tokenize("stats").unwrap();
        assert_eq!(tokens[0], Token::Stats);
    }

    #[test]
    fn tokenize_where() {
        let tokens = tokenize("SHOW category AT time:2022 WHERE region:US").unwrap();
        assert_eq!(tokens[4], Token::Where);
        assert_eq!(tokens[5], Token::DimRef("region".into(), "US".into()));
    }

    #[test]
    fn tokenize_top_bottom() {
        let tokens = tokenize("SHOW category AT time:2022 TOP 10").unwrap();
        assert_eq!(tokens[4], Token::Top);
        assert_eq!(tokens[5], Token::Number(10));

        let tokens = tokenize("SHOW category AT time:2022 BOTTOM 5").unwrap();
        assert_eq!(tokens[4], Token::Bottom);
        assert_eq!(tokens[5], Token::Number(5));
    }

    #[test]
    fn tokenize_across() {
        let tokens = tokenize("COMPARE category ACROSS time").unwrap();
        assert_eq!(tokens[0], Token::Compare);
        assert_eq!(tokens[2], Token::Across);
    }

    #[test]
    fn tokenize_surprise() {
        let tokens = tokenize("SURPRISE time:2024 UNDER time:2023 ON category").unwrap();
        assert_eq!(tokens[0], Token::Surprise);
        assert_eq!(tokens[1], Token::DimRef("time".into(), "2024".into()));
        assert_eq!(tokens[2], Token::Under);
        assert_eq!(tokens[3], Token::DimRef("time".into(), "2023".into()));
        assert_eq!(tokens[4], Token::On);
        assert_eq!(tokens[5], Token::Ident("category".into()));
    }

    #[test]
    fn tokenize_structure() {
        let tokens = tokenize("STRUCTURE AT time:2024").unwrap();
        assert_eq!(tokens[0], Token::Structure);
        assert_eq!(tokens[1], Token::At);
        assert_eq!(tokens[2], Token::DimRef("time".into(), "2024".into()));

        let tokens = tokenize("COMPARE STRUCTURE BETWEEN time:2023 AND time:2024").unwrap();
        assert_eq!(tokens[0], Token::Compare);
        assert_eq!(tokens[1], Token::Structure);
        assert_eq!(tokens[2], Token::Between);
    }

    #[test]
    fn tokenize_estimate() {
        let tokens = tokenize("ESTIMATE plan, churned AT time:2025-Q1").unwrap();
        assert_eq!(tokens[0], Token::Estimate);
        assert_eq!(tokens[1], Token::Ident("plan".into()));
        assert_eq!(tokens[2], Token::Comma);
        assert_eq!(tokens[3], Token::Ident("churned".into()));
        assert_eq!(tokens[4], Token::At);
        assert_eq!(tokens[5], Token::DimRef("time".into(), "2025-Q1".into()));
    }

    #[test]
    fn tokenize_audit_storage() {
        let tokens = tokenize("AUDIT STORAGE").unwrap();
        assert_eq!(tokens[0], Token::Audit);
        assert_eq!(tokens[1], Token::Storage);
    }

    #[test]
    fn tokenize_suggest() {
        let tokens = tokenize("SUGGEST LIMIT 5").unwrap();
        assert_eq!(tokens[0], Token::Suggest);
        assert_eq!(tokens[1], Token::Limit);
        assert_eq!(tokens[2], Token::Number(5));

        let tokens = tokenize("suggest").unwrap();
        assert_eq!(tokens[0], Token::Suggest);
    }

    #[test]
    fn tokenize_export() {
        let tokens = tokenize("EXPORT STATS AS CSV").unwrap();
        assert_eq!(tokens[0], Token::Export);
        assert_eq!(tokens[1], Token::Stats);
        assert_eq!(tokens[2], Token::As);
        assert_eq!(tokens[3], Token::Csv);
    }
}

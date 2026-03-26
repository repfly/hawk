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
        let (word, has_comma) = if word.ends_with(',') {
            (&word[..word.len() - 1], true)
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
    fn tokenize_export() {
        let tokens = tokenize("EXPORT STATS AS CSV").unwrap();
        assert_eq!(tokens[0], Token::Export);
        assert_eq!(tokens[1], Token::Stats);
        assert_eq!(tokens[2], Token::As);
        assert_eq!(tokens[3], Token::Csv);
    }
}

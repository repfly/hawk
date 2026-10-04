use crate::sql::tokenizer::Token;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// COMPARE <variable> BETWEEN <dim_ref> AND <dim_ref> [WHERE dim:val [AND dim:val ...]]
    Compare {
        variable: String,
        ref_a: DimRef,
        ref_b: DimRef,
        filters: Vec<DimRef>,
    },
    /// COMPARE <variable> ACROSS <dimension> [WHERE dim:val [AND dim:val ...]]
    CompareAll {
        variable: String,
        dimension: String,
        filters: Vec<DimRef>,
    },
    /// EXPLAIN <dim_ref> VS <dim_ref>
    Explain { ref_a: DimRef, ref_b: DimRef },
    /// SURPRISE <dim_ref> UNDER <dim_ref> [ON <variable>]
    Surprise {
        ref_a: DimRef,
        ref_b: DimRef,
        variable: Option<String>,
    },
    /// STRUCTURE AT <dim_ref>
    Structure { reference: DimRef },
    /// COMPARE STRUCTURE BETWEEN <dim_ref> AND <dim_ref>
    CompareStructure { ref_a: DimRef, ref_b: DimRef },
    /// ESTIMATE <var_a>, <var_b> AT <dim_ref>
    Estimate {
        var_a: String,
        var_b: String,
        reference: DimRef,
    },
    /// TRACK <variable> FROM <dim_ref> [GRANULARITY <ident>]
    Track {
        variable: String,
        reference: DimRef,
        granularity: Option<String>,
    },
    /// SHOW <variable> AT <dim_ref> [WHERE dim:val [AND dim:val ...]] [TOP <n> | BOTTOM <n>]
    Show {
        variable: String,
        reference: DimRef,
        filters: Vec<DimRef>,
        top_n: Option<usize>,
        bottom_n: Option<usize>,
    },
    /// RANK <variable> BY ENTROPY OVER <dimension> [WHERE dim:val [AND dim:val ...]]
    Rank {
        variable: String,
        dimension: String,
        filters: Vec<DimRef>,
    },
    /// MI <var_a>, <var_b> AT <dim_ref>
    MutualInfo {
        var_a: String,
        var_b: String,
        reference: DimRef,
    },
    /// CMI <var_a>, <var_b> GIVEN <dimension>
    ConditionalMI {
        var_a: String,
        var_b: String,
        dimension: String,
    },
    /// CORRELATIONS [OVER <dimension>] [LIMIT <n>]
    Correlations {
        dimension: Option<String>,
        limit: usize,
    },
    /// PAIRWISE <dimension> ON <variable> [USING <metric>]
    Pairwise {
        dimension: String,
        variable: String,
        metric: String,
    },
    /// NEAREST <dim_ref> ON <dimension> [LIMIT <n>] [USING <metric>]
    Nearest {
        reference: DimRef,
        dimension: String,
        limit: usize,
        metric: String,
    },
    /// EXPORT <inner_statement> AS CSV|JSON
    Export {
        inner: Box<Statement>,
        format: ExportFormat,
    },
    /// EXPORT DISTRIBUTION <variable> AT <dim_ref> AS JSON
    ExportDistribution { variable: String, reference: DimRef },
    /// ALERT WHEN <metric> <op> <value> ON <variable> [FROM <dim_ref>]
    Alert {
        metric: String,
        op: AlertOp,
        threshold: f64,
        variable: String,
        reference: Option<DimRef>,
    },
    /// AUDIT STORAGE — advisory MDL report: per-object storage cost vs.
    /// information retained. Read-only, mutates nothing.
    AuditStorage,
    /// SUGGEST [LIMIT <n>] — rank candidate next queries by expected
    /// information gain (default limit 10). The DSL path has no session
    /// history; the MCP `suggest` tool adds ledger-based dedup.
    Suggest { limit: usize },
    /// STATS
    Stats,
    /// SCHEMA
    Schema,
    /// DIMENSIONS [<name>]
    Dimensions { name: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlertOp {
    Gt,
    Lt,
    Gte,
    Lte,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExportFormat {
    Csv,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DimRef {
    pub dimension: String,
    pub value: String,
}

impl DimRef {
    pub fn to_ref_string(&self) -> String {
        format!("{}:{}", self.dimension, self.value)
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        let tok = self.tokens.get(self.pos).unwrap_or(&Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect_ident(&mut self) -> Result<String, String> {
        match self.advance().clone() {
            Token::Ident(s) => Ok(s),
            other => Err(format!("expected identifier, got {:?}", other)),
        }
    }

    fn expect_dim_ref(&mut self) -> Result<DimRef, String> {
        match self.advance().clone() {
            Token::DimRef(dim, val) => Ok(DimRef {
                dimension: dim,
                value: val,
            }),
            other => Err(format!("expected dimension:value, got {:?}", other)),
        }
    }

    fn expect_token(&mut self, expected: &Token) -> Result<(), String> {
        let got = self.advance().clone();
        if &got == expected {
            Ok(())
        } else {
            Err(format!("expected {:?}, got {:?}", expected, got))
        }
    }

    fn try_number(&mut self) -> Option<usize> {
        if let Token::Number(n) = self.peek() {
            let n = *n;
            self.advance();
            Some(n)
        } else {
            None
        }
    }

    /// Parse optional WHERE dim:val [AND dim:val ...] clause
    fn parse_where_clause(&mut self) -> Result<Vec<DimRef>, String> {
        let mut filters = Vec::new();
        if self.peek() == &Token::Where {
            self.advance();
            filters.push(self.expect_dim_ref()?);
            while self.peek() == &Token::And {
                self.advance();
                filters.push(self.expect_dim_ref()?);
            }
        }
        Ok(filters)
    }

    fn parse(&mut self) -> Result<Statement, String> {
        match self.advance().clone() {
            Token::Compare => self.parse_compare(),
            Token::Explain => self.parse_explain(),
            Token::Surprise => self.parse_surprise(),
            Token::Structure => self.parse_structure(),
            Token::Estimate => self.parse_estimate(),
            Token::Track => self.parse_track(),
            Token::Show => self.parse_show(),
            Token::Rank => self.parse_rank(),
            Token::Mi => self.parse_mi(),
            Token::Cmi => self.parse_cmi(),
            Token::Correlations => self.parse_correlations(),
            Token::Pairwise => self.parse_pairwise(),
            Token::Nearest => self.parse_nearest(),
            Token::Export => self.parse_export(),
            Token::Alert => self.parse_alert(),
            Token::Audit => self.parse_audit(),
            Token::Suggest => self.parse_suggest(),
            Token::Stats => Ok(Statement::Stats),
            Token::Schema => Ok(Statement::Schema),
            Token::Dimensions => self.parse_dimensions(),
            Token::Eof => Err("empty query".into()),
            other => Err(format!(
                "unexpected token {:?}; expected COMPARE, EXPLAIN, SURPRISE, STRUCTURE, ESTIMATE, \
                 TRACK, SHOW, RANK, MI, CMI, CORRELATIONS, PAIRWISE, NEAREST, EXPORT, ALERT, \
                 AUDIT, SUGGEST, STATS, SCHEMA, or DIMENSIONS",
                other
            )),
        }
    }

    // COMPARE <variable> BETWEEN <dim_ref> AND <dim_ref> [WHERE ...]
    // COMPARE <variable> ACROSS <dimension> [WHERE ...]
    // COMPARE STRUCTURE BETWEEN <dim_ref> AND <dim_ref>
    fn parse_compare(&mut self) -> Result<Statement, String> {
        if self.peek() == &Token::Structure {
            self.advance();
            self.expect_token(&Token::Between)?;
            let ref_a = self.expect_dim_ref()?;
            self.expect_token(&Token::And)?;
            let ref_b = self.expect_dim_ref()?;
            return Ok(Statement::CompareStructure { ref_a, ref_b });
        }

        let variable = self.expect_ident()?;

        match self.peek().clone() {
            Token::Between => {
                self.advance();
                let ref_a = self.expect_dim_ref()?;
                self.expect_token(&Token::And)?;
                let ref_b = self.expect_dim_ref()?;
                let filters = self.parse_where_clause()?;
                Ok(Statement::Compare {
                    variable,
                    ref_a,
                    ref_b,
                    filters,
                })
            }
            Token::Across => {
                self.advance();
                let dimension = self.expect_ident()?;
                let filters = self.parse_where_clause()?;
                Ok(Statement::CompareAll {
                    variable,
                    dimension,
                    filters,
                })
            }
            other => Err(format!(
                "expected BETWEEN or ACROSS after variable, got {:?}",
                other
            )),
        }
    }

    // EXPLAIN <dim_ref> VS <dim_ref>
    fn parse_explain(&mut self) -> Result<Statement, String> {
        let ref_a = self.expect_dim_ref()?;
        self.expect_token(&Token::Vs)?;
        let ref_b = self.expect_dim_ref()?;
        Ok(Statement::Explain { ref_a, ref_b })
    }

    // SURPRISE <dim_ref> UNDER <dim_ref> [ON <variable>]
    fn parse_surprise(&mut self) -> Result<Statement, String> {
        let ref_a = self.expect_dim_ref()?;
        self.expect_token(&Token::Under)?;
        let ref_b = self.expect_dim_ref()?;
        let variable = if self.peek() == &Token::On {
            self.advance();
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(Statement::Surprise {
            ref_a,
            ref_b,
            variable,
        })
    }

    // STRUCTURE AT <dim_ref>
    fn parse_structure(&mut self) -> Result<Statement, String> {
        self.expect_token(&Token::At)?;
        let reference = self.expect_dim_ref()?;
        Ok(Statement::Structure { reference })
    }

    // ESTIMATE <var_a>, <var_b> AT <dim_ref>
    fn parse_estimate(&mut self) -> Result<Statement, String> {
        let var_a = self.expect_ident()?;
        if self.peek() == &Token::Comma {
            self.advance();
        }
        let var_b = self.expect_ident()?;
        self.expect_token(&Token::At)?;
        let reference = self.expect_dim_ref()?;
        Ok(Statement::Estimate {
            var_a,
            var_b,
            reference,
        })
    }

    // TRACK <variable> FROM <dim_ref> [GRANULARITY <ident>]
    fn parse_track(&mut self) -> Result<Statement, String> {
        let variable = self.expect_ident()?;
        self.expect_token(&Token::From)?;
        let reference = self.expect_dim_ref()?;
        let granularity = if self.peek() == &Token::Granularity {
            self.advance();
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(Statement::Track {
            variable,
            reference,
            granularity,
        })
    }

    // SHOW <variable> AT <dim_ref> [WHERE ...] [TOP <n> | BOTTOM <n>]
    fn parse_show(&mut self) -> Result<Statement, String> {
        let variable = self.expect_ident()?;
        self.expect_token(&Token::At)?;
        let reference = self.expect_dim_ref()?;
        let filters = self.parse_where_clause()?;

        let mut top_n = None;
        let mut bottom_n = None;
        match self.peek() {
            Token::Top => {
                self.advance();
                top_n = Some(self.try_number().ok_or("expected number after TOP")?);
            }
            Token::Bottom => {
                self.advance();
                bottom_n = Some(self.try_number().ok_or("expected number after BOTTOM")?);
            }
            _ => {}
        }

        Ok(Statement::Show {
            variable,
            reference,
            filters,
            top_n,
            bottom_n,
        })
    }

    // RANK <variable> BY ENTROPY OVER <dimension> [WHERE ...]
    fn parse_rank(&mut self) -> Result<Statement, String> {
        let variable = self.expect_ident()?;
        self.expect_token(&Token::By)?;
        self.expect_token(&Token::Entropy)?;
        self.expect_token(&Token::Over)?;
        let dimension = self.expect_ident()?;
        let filters = self.parse_where_clause()?;
        Ok(Statement::Rank {
            variable,
            dimension,
            filters,
        })
    }

    // MI <var_a>, <var_b> AT <dim_ref>
    fn parse_mi(&mut self) -> Result<Statement, String> {
        let var_a = self.expect_ident()?;
        // optional comma
        if self.peek() == &Token::Comma {
            self.advance();
        }
        let var_b = self.expect_ident()?;
        self.expect_token(&Token::At)?;
        let reference = self.expect_dim_ref()?;
        Ok(Statement::MutualInfo {
            var_a,
            var_b,
            reference,
        })
    }

    // CMI <var_a>, <var_b> GIVEN <dimension>
    fn parse_cmi(&mut self) -> Result<Statement, String> {
        let var_a = self.expect_ident()?;
        if self.peek() == &Token::Comma {
            self.advance();
        }
        let var_b = self.expect_ident()?;
        self.expect_token(&Token::Given)?;
        let dimension = self.expect_ident()?;
        Ok(Statement::ConditionalMI {
            var_a,
            var_b,
            dimension,
        })
    }

    // CORRELATIONS [OVER <dimension>] [LIMIT <n>]
    fn parse_correlations(&mut self) -> Result<Statement, String> {
        let mut dimension = None;
        let mut limit = 20;

        loop {
            match self.peek() {
                Token::Over => {
                    self.advance();
                    dimension = Some(self.expect_ident()?);
                }
                Token::Limit => {
                    self.advance();
                    limit = self.try_number().ok_or("expected number after LIMIT")?;
                }
                _ => break,
            }
        }

        Ok(Statement::Correlations { dimension, limit })
    }

    // PAIRWISE <dimension> ON <variable> [USING <metric>]
    fn parse_pairwise(&mut self) -> Result<Statement, String> {
        let dimension = self.expect_ident()?;
        self.expect_token(&Token::On)?;
        let variable = self.expect_ident()?;
        let metric = if self.peek() == &Token::Using {
            self.advance();
            self.expect_ident()?
        } else {
            "jsd".to_owned()
        };
        Ok(Statement::Pairwise {
            dimension,
            variable,
            metric,
        })
    }

    // NEAREST <dim_ref> ON <dimension> [LIMIT <n>] [USING <metric>]
    fn parse_nearest(&mut self) -> Result<Statement, String> {
        let reference = self.expect_dim_ref()?;
        self.expect_token(&Token::On)?;
        let dimension = self.expect_ident()?;
        let mut limit = 3;
        let mut metric = "jsd".to_owned();

        loop {
            match self.peek() {
                Token::Limit => {
                    self.advance();
                    limit = self.try_number().ok_or("expected number after LIMIT")?;
                }
                Token::Using => {
                    self.advance();
                    metric = self.expect_ident()?;
                }
                _ => break,
            }
        }

        Ok(Statement::Nearest {
            reference,
            dimension,
            limit,
            metric,
        })
    }

    // ALERT WHEN <metric> <op> <value> ON <variable> [FROM <dim_ref>]
    fn parse_alert(&mut self) -> Result<Statement, String> {
        self.expect_token(&Token::When)?;
        let metric = self.expect_ident()?;
        let op = match self.advance().clone() {
            Token::Gt => AlertOp::Gt,
            Token::Lt => AlertOp::Lt,
            Token::Gte => AlertOp::Gte,
            Token::Lte => AlertOp::Lte,
            other => {
                return Err(format!(
                    "expected comparison operator (>, <, >=, <=), got {:?}",
                    other
                ))
            }
        };
        let threshold_str = self.expect_ident()?;
        let threshold: f64 = threshold_str
            .parse()
            .map_err(|_| format!("expected numeric threshold, got '{}'", threshold_str))?;
        self.expect_token(&Token::On)?;
        let variable = self.expect_ident()?;
        let reference = if self.peek() == &Token::From {
            self.advance();
            Some(self.expect_dim_ref()?)
        } else {
            None
        };
        Ok(Statement::Alert {
            metric,
            op,
            threshold,
            variable,
            reference,
        })
    }

    // AUDIT STORAGE
    fn parse_audit(&mut self) -> Result<Statement, String> {
        self.expect_token(&Token::Storage)?;
        Ok(Statement::AuditStorage)
    }

    // SUGGEST [LIMIT <n>]
    fn parse_suggest(&mut self) -> Result<Statement, String> {
        let mut limit = crate::query::suggest::DEFAULT_SUGGEST_LIMIT;
        if self.peek() == &Token::Limit {
            self.advance();
            limit = self.try_number().ok_or("expected number after LIMIT")?;
        }
        Ok(Statement::Suggest { limit })
    }

    // EXPORT DISTRIBUTION <variable> AT <dim_ref> AS JSON
    fn parse_export_distribution(&mut self) -> Result<Statement, String> {
        let variable = self.expect_ident()?;
        self.expect_token(&Token::At)?;
        let reference = self.expect_dim_ref()?;
        self.expect_token(&Token::As)?;
        match self.peek() {
            Token::Json => {
                self.advance();
                Ok(Statement::ExportDistribution {
                    variable,
                    reference,
                })
            }
            other => Err(format!(
                "expected JSON after AS for distribution export, got {:?}",
                other
            )),
        }
    }

    // EXPORT <inner_query> AS CSV|JSON
    fn parse_export(&mut self) -> Result<Statement, String> {
        // Check for EXPORT DISTRIBUTION ...
        if let Token::Ident(ref s) = *self.peek() {
            if s.eq_ignore_ascii_case("distribution") {
                self.advance();
                return self.parse_export_distribution();
            }
        }

        // Otherwise parse inner statement
        let inner = self.parse()?;

        self.expect_token(&Token::As)?;

        let format = match self.peek() {
            Token::Csv => {
                self.advance();
                ExportFormat::Csv
            }
            Token::Json => {
                self.advance();
                ExportFormat::Json
            }
            other => return Err(format!("expected CSV or JSON after AS, got {:?}", other)),
        };

        Ok(Statement::Export {
            inner: Box::new(inner),
            format,
        })
    }

    // DIMENSIONS [<name>]
    fn parse_dimensions(&mut self) -> Result<Statement, String> {
        let name = if let Token::Ident(_) = self.peek() {
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(Statement::Dimensions { name })
    }
}

pub fn parse(input: &str) -> Result<Statement, String> {
    let tokens = crate::sql::tokenizer::tokenize(input)?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_compare() {
        let stmt = parse("COMPARE category BETWEEN time:2013 AND time:2022").unwrap();
        assert_eq!(
            stmt,
            Statement::Compare {
                variable: "category".into(),
                ref_a: DimRef {
                    dimension: "time".into(),
                    value: "2013".into()
                },
                ref_b: DimRef {
                    dimension: "time".into(),
                    value: "2022".into()
                },
                filters: vec![],
            }
        );
    }

    #[test]
    fn parse_compare_with_where() {
        let stmt = parse(
            "COMPARE category BETWEEN time:2013 AND time:2022 WHERE region:US AND topic:politics",
        )
        .unwrap();
        match stmt {
            Statement::Compare { filters, .. } => {
                assert_eq!(filters.len(), 2);
                assert_eq!(filters[0].dimension, "region");
                assert_eq!(filters[0].value, "US");
                assert_eq!(filters[1].dimension, "topic");
                assert_eq!(filters[1].value, "politics");
            }
            _ => panic!("expected Compare"),
        }
    }

    #[test]
    fn parse_compare_all() {
        let stmt = parse("COMPARE category ACROSS time").unwrap();
        assert_eq!(
            stmt,
            Statement::CompareAll {
                variable: "category".into(),
                dimension: "time".into(),
                filters: vec![],
            }
        );
    }

    #[test]
    fn parse_compare_all_with_where() {
        let stmt = parse("COMPARE category ACROSS time WHERE region:US").unwrap();
        match stmt {
            Statement::CompareAll { filters, .. } => {
                assert_eq!(filters.len(), 1);
                assert_eq!(filters[0].dimension, "region");
            }
            _ => panic!("expected CompareAll"),
        }
    }

    #[test]
    fn parse_explain() {
        let stmt = parse("explain time:2013 vs time:2022").unwrap();
        assert!(matches!(stmt, Statement::Explain { .. }));
    }

    #[test]
    fn parse_surprise() {
        let stmt = parse("SURPRISE time:2024 UNDER time:2023").unwrap();
        assert_eq!(
            stmt,
            Statement::Surprise {
                ref_a: DimRef {
                    dimension: "time".into(),
                    value: "2024".into()
                },
                ref_b: DimRef {
                    dimension: "time".into(),
                    value: "2023".into()
                },
                variable: None,
            }
        );
    }

    #[test]
    fn parse_surprise_with_variable() {
        let stmt = parse("surprise time:2024 under time:2023 on category").unwrap();
        match stmt {
            Statement::Surprise { variable, .. } => {
                assert_eq!(variable, Some("category".into()));
            }
            _ => panic!("expected Surprise"),
        }
    }

    #[test]
    fn parse_export_surprise_as_json() {
        let stmt = parse("EXPORT SURPRISE time:2024 UNDER time:2023 ON category AS JSON").unwrap();
        match stmt {
            Statement::Export { inner, format } => {
                assert!(matches!(*inner, Statement::Surprise { .. }));
                assert_eq!(format, ExportFormat::Json);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_structure() {
        let stmt = parse("STRUCTURE AT time:2024").unwrap();
        assert_eq!(
            stmt,
            Statement::Structure {
                reference: DimRef {
                    dimension: "time".into(),
                    value: "2024".into()
                },
            }
        );
    }

    #[test]
    fn parse_compare_structure() {
        let stmt = parse("COMPARE STRUCTURE BETWEEN time:2023 AND time:2024").unwrap();
        assert_eq!(
            stmt,
            Statement::CompareStructure {
                ref_a: DimRef {
                    dimension: "time".into(),
                    value: "2023".into()
                },
                ref_b: DimRef {
                    dimension: "time".into(),
                    value: "2024".into()
                },
            }
        );
    }

    #[test]
    fn parse_compare_variable_still_works_alongside_compare_structure() {
        // COMPARE <var> BETWEEN ... must not be broken by COMPARE STRUCTURE.
        let stmt = parse("COMPARE category BETWEEN time:2023 AND time:2024").unwrap();
        assert!(matches!(stmt, Statement::Compare { ref variable, .. } if variable == "category"));

        let stmt = parse("COMPARE category ACROSS time").unwrap();
        assert!(matches!(stmt, Statement::CompareAll { .. }));

        let stmt = parse("compare structure between time:2023 and time:2024").unwrap();
        assert!(matches!(stmt, Statement::CompareStructure { .. }));
    }

    #[test]
    fn parse_export_structure_as_json() {
        let stmt = parse("EXPORT STRUCTURE AT time:2024 AS JSON").unwrap();
        match stmt {
            Statement::Export { inner, format } => {
                assert!(matches!(*inner, Statement::Structure { .. }));
                assert_eq!(format, ExportFormat::Json);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_export_compare_structure_as_csv() {
        let stmt =
            parse("EXPORT COMPARE STRUCTURE BETWEEN time:2023 AND time:2024 AS CSV").unwrap();
        match stmt {
            Statement::Export { inner, format } => {
                assert!(matches!(*inner, Statement::CompareStructure { .. }));
                assert_eq!(format, ExportFormat::Csv);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_estimate() {
        let stmt = parse("ESTIMATE plan, churned AT time:2025-Q1").unwrap();
        assert_eq!(
            stmt,
            Statement::Estimate {
                var_a: "plan".into(),
                var_b: "churned".into(),
                reference: DimRef {
                    dimension: "time".into(),
                    value: "2025-Q1".into()
                },
            }
        );
        // Comma is optional, matching MI.
        let stmt = parse("estimate plan churned at time:2025-Q1").unwrap();
        assert!(matches!(stmt, Statement::Estimate { .. }));
    }

    #[test]
    fn parse_export_estimate_as_json() {
        let stmt = parse("EXPORT ESTIMATE plan, churned AT time:2025-Q1 AS JSON").unwrap();
        match stmt {
            Statement::Export { inner, format } => {
                assert!(matches!(*inner, Statement::Estimate { .. }));
                assert_eq!(format, ExportFormat::Json);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_track_with_granularity() {
        let stmt = parse("TRACK category FROM time:2012 GRANULARITY yearly").unwrap();
        match stmt {
            Statement::Track { granularity, .. } => assert_eq!(granularity, Some("yearly".into())),
            _ => panic!("expected Track"),
        }
    }

    #[test]
    fn parse_show() {
        let stmt = parse("SHOW category AT time:2022").unwrap();
        match stmt {
            Statement::Show {
                variable,
                filters,
                top_n,
                bottom_n,
                ..
            } => {
                assert_eq!(variable, "category");
                assert!(filters.is_empty());
                assert!(top_n.is_none());
                assert!(bottom_n.is_none());
            }
            _ => panic!("expected Show"),
        }
    }

    #[test]
    fn parse_show_with_where() {
        let stmt = parse("SHOW category AT time:2022 WHERE region:US").unwrap();
        match stmt {
            Statement::Show { filters, .. } => {
                assert_eq!(filters.len(), 1);
                assert_eq!(filters[0].dimension, "region");
                assert_eq!(filters[0].value, "US");
            }
            _ => panic!("expected Show"),
        }
    }

    #[test]
    fn parse_show_top() {
        let stmt = parse("SHOW category AT time:2022 TOP 10").unwrap();
        match stmt {
            Statement::Show {
                top_n, bottom_n, ..
            } => {
                assert_eq!(top_n, Some(10));
                assert!(bottom_n.is_none());
            }
            _ => panic!("expected Show"),
        }
    }

    #[test]
    fn parse_show_bottom() {
        let stmt = parse("SHOW category AT time:2022 BOTTOM 5").unwrap();
        match stmt {
            Statement::Show {
                top_n, bottom_n, ..
            } => {
                assert!(top_n.is_none());
                assert_eq!(bottom_n, Some(5));
            }
            _ => panic!("expected Show"),
        }
    }

    #[test]
    fn parse_show_where_and_top() {
        let stmt = parse("SHOW category AT time:2022 WHERE region:US TOP 5").unwrap();
        match stmt {
            Statement::Show { filters, top_n, .. } => {
                assert_eq!(filters.len(), 1);
                assert_eq!(top_n, Some(5));
            }
            _ => panic!("expected Show"),
        }
    }

    #[test]
    fn parse_rank_with_where() {
        let stmt = parse("RANK category BY ENTROPY OVER time WHERE region:US").unwrap();
        match stmt {
            Statement::Rank { filters, .. } => {
                assert_eq!(filters.len(), 1);
                assert_eq!(filters[0].dimension, "region");
            }
            _ => panic!("expected Rank"),
        }
    }

    #[test]
    fn parse_mi() {
        let stmt = parse("MI author, category AT time:2022").unwrap();
        assert!(matches!(stmt, Statement::MutualInfo { .. }));
    }

    #[test]
    fn parse_correlations_with_options() {
        let stmt = parse("CORRELATIONS OVER time LIMIT 10").unwrap();
        match stmt {
            Statement::Correlations { dimension, limit } => {
                assert_eq!(dimension, Some("time".into()));
                assert_eq!(limit, 10);
            }
            _ => panic!("expected Correlations"),
        }
    }

    #[test]
    fn parse_stats() {
        assert_eq!(parse("STATS").unwrap(), Statement::Stats);
    }

    #[test]
    fn parse_schema() {
        assert_eq!(parse("SCHEMA").unwrap(), Statement::Schema);
    }

    #[test]
    fn parse_nearest() {
        let stmt = parse("NEAREST time:2022 ON time LIMIT 5 USING hellinger").unwrap();
        match stmt {
            Statement::Nearest { limit, metric, .. } => {
                assert_eq!(limit, 5);
                assert_eq!(metric, "hellinger");
            }
            _ => panic!("expected Nearest"),
        }
    }

    #[test]
    fn parse_export_csv() {
        let stmt = parse("EXPORT STATS AS CSV").unwrap();
        match stmt {
            Statement::Export { inner, format } => {
                assert_eq!(*inner, Statement::Stats);
                assert_eq!(format, ExportFormat::Csv);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_export_json() {
        let stmt = parse("EXPORT SCHEMA AS JSON").unwrap();
        match stmt {
            Statement::Export { inner, format } => {
                assert_eq!(*inner, Statement::Schema);
                assert_eq!(format, ExportFormat::Json);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_audit_storage() {
        assert_eq!(parse("AUDIT STORAGE").unwrap(), Statement::AuditStorage);
        assert_eq!(parse("audit storage").unwrap(), Statement::AuditStorage);
        assert!(parse("AUDIT").is_err());
    }

    #[test]
    fn parse_export_audit_storage_as_json() {
        let stmt = parse("EXPORT AUDIT STORAGE AS JSON").unwrap();
        match stmt {
            Statement::Export { inner, format } => {
                assert_eq!(*inner, Statement::AuditStorage);
                assert_eq!(format, ExportFormat::Json);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_suggest() {
        assert_eq!(parse("SUGGEST").unwrap(), Statement::Suggest { limit: 10 });
        assert_eq!(
            parse("suggest limit 5").unwrap(),
            Statement::Suggest { limit: 5 }
        );
        assert!(parse("SUGGEST LIMIT").is_err());
    }

    #[test]
    fn parse_export_suggest_as_json() {
        let stmt = parse("EXPORT SUGGEST LIMIT 3 AS JSON").unwrap();
        match stmt {
            Statement::Export { inner, format } => {
                assert_eq!(*inner, Statement::Suggest { limit: 3 });
                assert_eq!(format, ExportFormat::Json);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parse_export_show_as_csv() {
        let stmt = parse("EXPORT SHOW category AT time:2022 AS CSV").unwrap();
        match stmt {
            Statement::Export { inner, format } => {
                assert!(matches!(*inner, Statement::Show { .. }));
                assert_eq!(format, ExportFormat::Csv);
            }
            _ => panic!("expected Export"),
        }
    }
}

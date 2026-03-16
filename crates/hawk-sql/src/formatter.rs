use std::fmt;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub header: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl QueryResult {
    /// Convert the result to CSV format.
    pub fn to_csv(&self) -> String {
        let mut out = String::new();

        // Header row
        for (i, h) in self.header.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&csv_escape(h));
        }
        out.push('\n');

        // Data rows
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&csv_escape(cell));
            }
            out.push('\n');
        }

        out
    }

    /// Convert the result to JSON format (array of objects keyed by header).
    pub fn to_json(&self) -> String {
        let mut out = String::from("[\n");

        for (row_idx, row) in self.rows.iter().enumerate() {
            out.push_str("  {");
            for (i, cell) in row.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                let key = self.header.get(i).map(|s| s.as_str()).unwrap_or("");
                out.push_str(&format!("\"{}\": \"{}\"", json_escape(key), json_escape(cell)));
            }
            out.push('}');
            if row_idx + 1 < self.rows.len() {
                out.push(',');
            }
            out.push('\n');
        }

        out.push(']');
        out
    }
}

/// Escape a field for CSV output. Wraps in quotes if necessary.
fn csv_escape(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_owned()
    }
}

/// Escape a string for JSON output.
fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.header.is_empty() {
            return Ok(());
        }

        let ncols = self.header.len();

        // Compute column widths
        let mut widths = vec![0usize; ncols];
        for (i, h) in self.header.iter().enumerate() {
            widths[i] = widths[i].max(h.len());
        }
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i < ncols {
                    widths[i] = widths[i].max(cell.len());
                }
            }
        }

        // Header
        for (i, h) in self.header.iter().enumerate() {
            if i > 0 {
                write!(f, "  ")?;
            }
            write!(f, "{:<width$}", h, width = widths[i])?;
        }
        writeln!(f)?;

        // Separator
        for (i, w) in widths.iter().enumerate() {
            if i > 0 {
                write!(f, "  ")?;
            }
            write!(f, "{}", "─".repeat(*w))?;
        }
        writeln!(f)?;

        // Rows
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i >= ncols {
                    break;
                }
                if i > 0 {
                    write!(f, "  ")?;
                }
                write!(f, "{:<width$}", cell, width = widths[i])?;
            }
            writeln!(f)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_simple_table() {
        let result = QueryResult {
            header: vec!["Name".into(), "Value".into()],
            rows: vec![
                vec!["JSD".into(), "0.1234".into()],
                vec!["PSI".into(), "0.5678".into()],
            ],
        };
        let output = result.to_string();
        assert!(output.contains("JSD"));
        assert!(output.contains("0.1234"));
        assert!(output.contains("─"));
    }

    #[test]
    fn to_csv_basic() {
        let result = QueryResult {
            header: vec!["Name".into(), "Value".into()],
            rows: vec![
                vec!["JSD".into(), "0.1234".into()],
                vec!["PSI".into(), "0.5678".into()],
            ],
        };
        let csv = result.to_csv();
        assert_eq!(csv, "Name,Value\nJSD,0.1234\nPSI,0.5678\n");
    }

    #[test]
    fn to_csv_with_commas() {
        let result = QueryResult {
            header: vec!["Metric".into(), "Value".into()],
            rows: vec![vec!["Samples".into(), "100, 200".into()]],
        };
        let csv = result.to_csv();
        assert!(csv.contains("\"100, 200\""));
    }

    #[test]
    fn to_json_basic() {
        let result = QueryResult {
            header: vec!["Name".into(), "Value".into()],
            rows: vec![
                vec!["JSD".into(), "0.1234".into()],
                vec!["PSI".into(), "0.5678".into()],
            ],
        };
        let json = result.to_json();
        assert!(json.starts_with('['));
        assert!(json.ends_with(']'));
        assert!(json.contains("\"Name\": \"JSD\""));
        assert!(json.contains("\"Value\": \"0.1234\""));
    }

    #[test]
    fn to_json_with_special_chars() {
        let result = QueryResult {
            header: vec!["Metric".into()],
            rows: vec![vec!["KL(A→B)".into()]],
        };
        let json = result.to_json();
        assert!(json.contains("Metric"));
    }
}

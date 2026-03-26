use anyhow::{anyhow, Result};

use crate::query::QueryEngine;
use crate::storage::Database;

use crate::sql::formatter::QueryResult;
use crate::sql::parser::{DimRef, ExportFormat, Statement};

pub fn execute(db: &Database, engine: &QueryEngine, stmt: &Statement) -> Result<QueryResult> {
    match stmt {
        Statement::Compare {
            variable,
            ref_a,
            ref_b,
            filters,
        } => exec_compare(db, engine, variable, ref_a, ref_b, filters),

        Statement::CompareAll {
            variable,
            dimension,
            filters,
        } => exec_compare_all(db, engine, variable, dimension, filters),

        Statement::Explain { ref_a, ref_b } => exec_explain(db, engine, ref_a, ref_b),

        Statement::Track {
            variable,
            reference,
            granularity,
        } => exec_track(db, engine, variable, reference, granularity.as_deref()),

        Statement::Show {
            variable,
            reference,
            filters,
            top_n,
            bottom_n,
        } => exec_show(db, variable, reference, filters, *top_n, *bottom_n),

        Statement::Rank {
            variable,
            dimension,
            filters,
        } => exec_rank(db, variable, dimension, filters),

        Statement::MutualInfo {
            var_a,
            var_b,
            reference,
        } => exec_mi(db, engine, var_a, var_b, reference),

        Statement::ConditionalMI {
            var_a,
            var_b,
            dimension,
        } => exec_cmi(db, engine, var_a, var_b, dimension),

        Statement::Correlations { dimension, limit } => {
            exec_correlations(db, engine, dimension.as_deref(), *limit)
        }

        Statement::Pairwise {
            dimension,
            variable,
            metric,
        } => exec_pairwise(db, engine, dimension, variable, metric),

        Statement::Nearest {
            reference,
            dimension,
            limit,
            metric,
        } => exec_nearest(db, engine, reference, dimension, *limit, metric),

        Statement::Export { inner, format } => exec_export(db, engine, inner, format),

        Statement::Stats => exec_stats(db),
        Statement::Schema => exec_schema(db),
        Statement::Dimensions { name } => exec_dimensions(db, name.as_deref()),
    }
}

/// Build a dimension key from a primary DimRef plus optional filter DimRefs.
fn build_dim_key(reference: &DimRef, filters: &[DimRef]) -> crate::core::DimensionKey {
    let pairs = std::iter::once((reference.dimension.clone(), reference.value.clone()))
        .chain(filters.iter().map(|f| (f.dimension.clone(), f.value.clone())));
    crate::core::dimension_key_from_pairs(pairs)
}

/// Build a ref string that includes filter dimensions (for engine calls).
fn build_ref_string(reference: &DimRef, filters: &[DimRef]) -> String {
    let mut s = reference.to_ref_string();
    for f in filters {
        s.push('/');
        s.push_str(&f.to_ref_string());
    }
    s
}

fn exec_compare(
    db: &Database,
    engine: &QueryEngine,
    variable: &str,
    ref_a: &DimRef,
    ref_b: &DimRef,
    filters: &[DimRef],
) -> Result<QueryResult> {
    let ref_a_str = build_ref_string(ref_a, filters);
    let ref_b_str = build_ref_string(ref_b, filters);

    let result = engine.compare(
        db,
        &ref_a_str,
        &ref_b_str,
        Some(variable),
    )?;

    let mut rows = vec![
        vec!["JSD".into(), format!("{:.6}", result.jsd)],
        vec!["PSI".into(), format!("{:.6}", result.psi)],
        vec!["Hellinger".into(), format!("{:.6}", result.hellinger)],
        vec!["KL(A→B)".into(), format!("{:.6}", result.kl_a_to_b)],
        vec!["KL(B→A)".into(), format!("{:.6}", result.kl_b_to_a)],
        vec!["Entropy(A)".into(), format!("{:.4} bits", result.entropy_a)],
        vec!["Entropy(B)".into(), format!("{:.4} bits", result.entropy_b)],
        vec![
            "Samples".into(),
            format!("{} vs {}", result.sample_count_a, result.sample_count_b),
        ],
        vec![
            "95% CI".into(),
            format!(
                "[{:.4}, {:.4}]",
                result.confidence.jsd_ci_lower, result.confidence.jsd_ci_upper
            ),
        ],
    ];

    if let Some(w) = result.wasserstein {
        rows.push(vec!["Wasserstein".into(), format!("{:.6}", w)]);
    }

    // Top movers
    if !result.top_movers.is_empty() {
        rows.push(vec!["".into(), "".into()]);
        rows.push(vec!["--- Top Movers ---".into(), "".into()]);
        for m in result.top_movers.iter().take(10) {
            rows.push(vec![
                m.category.clone(),
                format!(
                    "{:+.4}  ({:.3} → {:.3})  contrib={:.4}",
                    m.delta, m.prob_a, m.prob_b, m.contribution
                ),
            ]);
        }
    }

    Ok(QueryResult {
        header: vec!["Metric".into(), "Value".into()],
        rows,
    })
}

fn exec_compare_all(
    db: &Database,
    engine: &QueryEngine,
    variable: &str,
    dimension: &str,
    filters: &[DimRef],
) -> Result<QueryResult> {
    let values: Vec<String> = db.dimension_values(dimension).into_iter().collect();

    if values.len() < 2 {
        return Err(anyhow!(
            "dimension '{}' has fewer than 2 values for pairwise comparison",
            dimension
        ));
    }

    let mut rows = Vec::new();

    for i in 0..values.len() {
        for j in (i + 1)..values.len() {
            let ref_a = DimRef {
                dimension: dimension.to_owned(),
                value: values[i].clone(),
            };
            let ref_b = DimRef {
                dimension: dimension.to_owned(),
                value: values[j].clone(),
            };

            let ref_a_str = build_ref_string(&ref_a, filters);
            let ref_b_str = build_ref_string(&ref_b, filters);

            match engine.compare(db, &ref_a_str, &ref_b_str, Some(variable)) {
                Ok(result) => {
                    rows.push(vec![
                        values[i].clone(),
                        values[j].clone(),
                        format!("{:.6}", result.jsd),
                        format!("{:.6}", result.hellinger),
                        format!("{:.6}", result.psi),
                        format!(
                            "{} vs {}",
                            result.sample_count_a, result.sample_count_b
                        ),
                    ]);
                }
                Err(_) => {
                    // Skip pairs that fail (e.g., missing distributions)
                    rows.push(vec![
                        values[i].clone(),
                        values[j].clone(),
                        "N/A".into(),
                        "N/A".into(),
                        "N/A".into(),
                        "N/A".into(),
                    ]);
                }
            }
        }
    }

    // Sort by JSD descending for quick insight
    rows.sort_by(|a, b| {
        let jsd_a = a[2].parse::<f64>().unwrap_or(0.0);
        let jsd_b = b[2].parse::<f64>().unwrap_or(0.0);
        jsd_b.total_cmp(&jsd_a)
    });

    Ok(QueryResult {
        header: vec![
            "Value A".into(),
            "Value B".into(),
            "JSD".into(),
            "Hellinger".into(),
            "PSI".into(),
            "Samples".into(),
        ],
        rows,
    })
}

fn exec_explain(
    db: &Database,
    engine: &QueryEngine,
    ref_a: &DimRef,
    ref_b: &DimRef,
) -> Result<QueryResult> {
    let result = engine.explain(db, &ref_a.to_ref_string(), &ref_b.to_ref_string())?;

    let mut rows = vec![vec![
        "TOTAL".into(),
        format!("{:.6}", result.total_divergence),
        "100.0%".into(),
        "".into(),
        "".into(),
    ]];

    for c in &result.contributions {
        rows.push(vec![
            c.variable.clone(),
            format!("{:.6}", c.jsd),
            format!("{:.1}%", c.fraction * 100.0),
            format!("{:.4}", c.entropy_a),
            format!("{:.4}", c.entropy_b),
        ]);

        // Show top 5 movers per variable
        for m in c.top_movers.iter().take(5) {
            rows.push(vec![
                format!("  {}", m.category),
                format!("{:+.4}", m.delta),
                format!("contrib={:.4}", m.contribution),
                "".into(),
                "".into(),
            ]);
        }
    }

    Ok(QueryResult {
        header: vec![
            "Variable".into(),
            "JSD".into(),
            "Fraction".into(),
            "H(A)".into(),
            "H(B)".into(),
        ],
        rows,
    })
}

fn exec_track(
    db: &Database,
    engine: &QueryEngine,
    _variable: &str,
    reference: &DimRef,
    granularity: Option<&str>,
) -> Result<QueryResult> {
    let result = engine.track(
        db,
        &reference.to_ref_string(),
        None,
        None,
        granularity,
    )?;

    let mut rows = Vec::new();
    for (i, tp) in result.time_points.iter().enumerate() {
        let ent = result.entropy_series[i];
        let drift = if i < result.drift_series.len() {
            result.drift_series[i]
        } else {
            0.0
        };
        let flag = if drift > 0.05 { " ← shift" } else { "" };
        rows.push(vec![
            tp.clone(),
            format!("{:.4}", ent),
            format!("{:.4}{}", drift, flag),
        ]);
    }

    if !result.drift_events.is_empty() {
        rows.push(vec!["".into(), "".into(), "".into()]);
        for ev in &result.drift_events {
            rows.push(vec![
                format!("{} → {}", ev.time_from, ev.time_to),
                format!("JSD={:.4}", ev.jsd),
                ev.description.clone(),
            ]);
        }
    }

    Ok(QueryResult {
        header: vec!["Time".into(), "Entropy".into(), "Drift (JSD)".into()],
        rows,
    })
}

fn exec_show(
    db: &Database,
    variable: &str,
    reference: &DimRef,
    filters: &[DimRef],
    top_n: Option<usize>,
    bottom_n: Option<usize>,
) -> Result<QueryResult> {
    let dim_key = build_dim_key(reference, filters);
    let dist = db
        .get_distribution(variable, &dim_key)
        .ok_or_else(|| anyhow!("distribution not found"))?;

    let mut rows = vec![
        vec!["Entropy".into(), format!("{:.4} bits", dist.entropy)],
        vec!["Samples".into(), format!("{}", dist.sample_count)],
        vec!["Version".into(), format!("{}", dist.version)],
        vec!["".into(), "".into()],
    ];

    // Collect category/bin rows with their probability for sorting
    let mut cat_rows: Vec<(f64, Vec<String>)> = Vec::new();

    match &dist.repr {
        crate::core::DistributionRepr::Categorical {
            categories, counts, total_count, ..
        } => {
            for (cat, count) in categories.iter().zip(counts.iter()) {
                let prob = if *total_count > 0 {
                    *count as f64 / *total_count as f64
                } else {
                    0.0
                };
                let bar = "#".repeat((prob * 40.0) as usize);
                cat_rows.push((
                    prob,
                    vec![
                        cat.clone(),
                        format!("{:6}  {:.4}  {}", count, prob, bar),
                    ],
                ));
            }
        }
        crate::core::DistributionRepr::Histogram {
            min, max, bin_counts, total_count,
        } => {
            let n = bin_counts.len();
            let width = (max - min) / n as f64;
            for (i, count) in bin_counts.iter().enumerate() {
                let lo = min + i as f64 * width;
                let hi = lo + width;
                let prob = if *total_count > 0 {
                    *count as f64 / *total_count as f64
                } else {
                    0.0
                };
                let bar = "#".repeat((prob * 40.0) as usize);
                cat_rows.push((
                    prob,
                    vec![
                        format!("[{:.2}, {:.2})", lo, hi),
                        format!("{:6}  {:.4}  {}", count, prob, bar),
                    ],
                ));
            }
        }
    }

    // Apply TOP N or BOTTOM N
    if top_n.is_some() || bottom_n.is_some() {
        // Sort by probability descending
        cat_rows.sort_by(|a, b| b.0.total_cmp(&a.0));

        if let Some(n) = top_n {
            cat_rows.truncate(n);
        } else if let Some(n) = bottom_n {
            // Sort ascending for bottom, then take first n
            cat_rows.sort_by(|a, b| a.0.total_cmp(&b.0));
            cat_rows.truncate(n);
        }
    }

    for (_prob, row) in cat_rows {
        rows.push(row);
    }

    Ok(QueryResult {
        header: vec!["Category/Bin".into(), "Count / Prob".into()],
        rows,
    })
}

fn exec_rank(
    db: &Database,
    variable: &str,
    dimension: &str,
    filters: &[DimRef],
) -> Result<QueryResult> {
    let mut ranked: Vec<_> = db
        .distributions_for_variable(variable)
        .into_iter()
        .filter_map(|d| {
            // Check that all filters match
            for f in filters {
                match d.dimension_key.get(&f.dimension) {
                    Some(v) if v == &f.value => {}
                    _ => return None,
                }
            }
            d.dimension_key
                .get(dimension)
                .map(|v| (v.clone(), d.entropy, d.sample_count))
        })
        .collect();

    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));

    let rows = ranked
        .iter()
        .map(|(val, ent, count)| {
            let bar = "#".repeat((*ent * 8.0) as usize);
            vec![
                val.clone(),
                format!("{:.4} bits", ent),
                format!("{}", count),
                bar,
            ]
        })
        .collect();

    Ok(QueryResult {
        header: vec![
            dimension.to_owned(),
            "Entropy".into(),
            "Samples".into(),
            "".into(),
        ],
        rows,
    })
}

fn exec_mi(
    db: &Database,
    engine: &QueryEngine,
    var_a: &str,
    var_b: &str,
    reference: &DimRef,
) -> Result<QueryResult> {
    let mi = engine.mutual_info(db, var_a, var_b, &reference.to_ref_string())?;

    // Also get joint to compute cramers_v
    let dim_key = crate::core::dimension_key_from_pairs(std::iter::once((
        reference.dimension.clone(),
        reference.value.clone(),
    )));
    let joint = db.get_joint_distribution(var_a, var_b, &dim_key);

    let mut rows = vec![vec!["MI".into(), format!("{:.4} bits", mi)]];

    if let Some(j) = joint {
        let (counts, total) = extract_joint_counts(j);
        let nmi = crate::math::normalized_mutual_information(&counts, total);
        let cv = crate::math::cramers_v(&counts, total);
        rows.push(vec!["NMI".into(), format!("{:.4}", nmi)]);
        rows.push(vec!["Cramér's V".into(), format!("{:.4}", cv)]);
        rows.push(vec!["Samples".into(), format!("{}", total)]);
    }

    let strength = if mi > 0.3 {
        "strong"
    } else if mi > 0.1 {
        "moderate"
    } else {
        "weak"
    };
    rows.push(vec!["Strength".into(), strength.into()]);

    Ok(QueryResult {
        header: vec!["Metric".into(), "Value".into()],
        rows,
    })
}

fn exec_cmi(
    db: &Database,
    engine: &QueryEngine,
    var_a: &str,
    var_b: &str,
    dimension: &str,
) -> Result<QueryResult> {
    let result = engine.conditional_mutual_info(db, var_a, var_b, dimension, None)?;

    let mut rows = vec![
        vec![
            "CMI".into(),
            format!("{:.4} bits", result.cmi),
            "".into(),
            "".into(),
        ],
        vec![
            "Total samples".into(),
            format!("{}", result.total_samples),
            "".into(),
            "".into(),
        ],
        vec!["".into(), "".into(), "".into(), "".into()],
    ];

    for pv in &result.per_value {
        rows.push(vec![
            pv.value.clone(),
            format!("{:.4}", pv.mi),
            format!("{:.1}%", pv.nmi * 100.0),
            format!("{}", pv.sample_count),
        ]);
    }

    Ok(QueryResult {
        header: vec![
            dimension.to_owned(),
            "MI".into(),
            "NMI".into(),
            "Samples".into(),
        ],
        rows,
    })
}

fn exec_correlations(
    db: &Database,
    engine: &QueryEngine,
    dimension: Option<&str>,
    limit: usize,
) -> Result<QueryResult> {
    let result = engine.discover_correlations(db, dimension, limit)?;

    let rows = result
        .pairs
        .iter()
        .map(|p| {
            vec![
                format!("{} × {}", p.var_a, p.var_b),
                p.dimension_value.clone().unwrap_or_else(|| "all".into()),
                format!("{:.4}", p.mi),
                format!("{:.1}%", p.nmi * 100.0),
                format!("{:.4}", p.cramers_v),
                format!("{}", p.sample_count),
            ]
        })
        .collect();

    Ok(QueryResult {
        header: vec![
            "Pair".into(),
            "Dim".into(),
            "MI".into(),
            "NMI".into(),
            "Cramér's V".into(),
            "Samples".into(),
        ],
        rows,
    })
}

fn exec_pairwise(
    db: &Database,
    engine: &QueryEngine,
    dimension: &str,
    variable: &str,
    metric: &str,
) -> Result<QueryResult> {
    let (labels, matrix) = engine.pairwise(db, dimension, variable, metric)?;

    let mut header = vec!["".into()];
    header.extend(labels.iter().cloned());

    let rows = labels
        .iter()
        .enumerate()
        .map(|(i, label)| {
            let mut row = vec![label.clone()];
            for j in 0..labels.len() {
                row.push(if i == j {
                    "—".into()
                } else {
                    format!("{:.4}", matrix[i][j])
                });
            }
            row
        })
        .collect();

    Ok(QueryResult { header, rows })
}

fn exec_nearest(
    db: &Database,
    engine: &QueryEngine,
    reference: &DimRef,
    dimension: &str,
    limit: usize,
    metric: &str,
) -> Result<QueryResult> {
    let parsed = crate::query::parser::parse_reference(&reference.to_ref_string())
        .map_err(|e| anyhow!(e.to_string()))?;
    let variable = parsed
        .variable
        .or_else(|| db.schema().first_variable_name().map(ToOwned::to_owned))
        .ok_or_else(|| anyhow!("no variable in schema"))?;

    let values: Vec<String> = db.dimension_values(dimension).into_iter().collect();

    let mut neighbors = Vec::new();
    for value in &values {
        if value == &reference.value {
            continue;
        }
        let other_ref = format!("{}:{}", dimension, value);
        let cmp = engine.compare(db, &reference.to_ref_string(), &other_ref, Some(&variable))?;
        let dist = if metric.eq_ignore_ascii_case("hellinger") {
            cmp.hellinger
        } else if metric.eq_ignore_ascii_case("psi") {
            cmp.psi
        } else {
            cmp.jsd
        };
        neighbors.push((value.clone(), dist));
    }

    neighbors.sort_by(|a, b| a.1.total_cmp(&b.1));
    neighbors.truncate(limit);

    let rows = neighbors
        .iter()
        .map(|(val, dist)| vec![val.clone(), format!("{:.6}", dist)])
        .collect();

    Ok(QueryResult {
        header: vec![dimension.to_owned(), metric.to_uppercase()],
        rows,
    })
}

fn exec_export(
    db: &Database,
    engine: &QueryEngine,
    inner: &Statement,
    format: &ExportFormat,
) -> Result<QueryResult> {
    let inner_result = execute(db, engine, inner)?;

    let output = match format {
        ExportFormat::Csv => inner_result.to_csv(),
        ExportFormat::Json => inner_result.to_json(),
    };

    Ok(QueryResult {
        header: vec!["Output".into()],
        rows: vec![vec![output]],
    })
}

fn exec_stats(db: &Database) -> Result<QueryResult> {
    let stats = db.stats();
    Ok(QueryResult {
        header: vec!["Stat".into(), "Value".into()],
        rows: vec![
            vec!["Distributions".into(), format!("{}", stats.distributions)],
            vec!["Total samples".into(), format!("{}", stats.total_samples)],
            vec!["Variables".into(), format!("{}", stats.variables)],
            vec!["Dimensions".into(), format!("{}", stats.dimensions)],
        ],
    })
}

fn exec_schema(db: &Database) -> Result<QueryResult> {
    let schema = db.schema();
    let mut rows: Vec<Vec<String>> = Vec::new();

    for v in &schema.variables {
        let desc = match &v.var_type {
            crate::core::VariableType::Continuous { bins, range } => {
                let r = range.map(|(a, b)| format!("[{}, {}]", a, b)).unwrap_or_default();
                format!("continuous  bins={}  range={}", bins, r)
            }
            crate::core::VariableType::Categorical { categories, allow_unknown } => {
                format!(
                    "categorical  cats={}  unknown={}",
                    categories.len(),
                    allow_unknown
                )
            }
        };
        rows.push(vec!["variable".into(), v.name.clone(), desc]);
    }

    for d in &schema.dimensions {
        let gran = d.granularity.as_deref().unwrap_or("none");
        rows.push(vec![
            "dimension".into(),
            d.name.clone(),
            format!("source={}  granularity={}", d.source_column, gran),
        ]);
    }

    for (a, b) in &schema.joints {
        rows.push(vec!["joint".into(), format!("{} × {}", a, b), "".into()]);
    }

    Ok(QueryResult {
        header: vec!["Type".into(), "Name".into(), "Details".into()],
        rows,
    })
}

fn exec_dimensions(db: &Database, name: Option<&str>) -> Result<QueryResult> {
    if let Some(dim_name) = name {
        let values: Vec<String> = db.dimension_values(dim_name).into_iter().collect();
        let rows = values.iter().map(|v| vec![v.clone()]).collect();
        Ok(QueryResult {
            header: vec![dim_name.to_owned()],
            rows,
        })
    } else {
        let rows = db
            .schema()
            .dimensions
            .iter()
            .map(|d| {
                let vals = db.dimension_values(&d.name);
                vec![d.name.clone(), format!("{} values", vals.len())]
            })
            .collect();
        Ok(QueryResult {
            header: vec!["Dimension".into(), "Cardinality".into()],
            rows,
        })
    }
}

fn extract_joint_counts(joint: &crate::core::JointDistributionObject) -> (Vec<Vec<u64>>, u64) {
    use crate::core::JointRepr;
    match &joint.repr {
        JointRepr::HistogramGrid { counts, total_count, .. } => (counts.clone(), *total_count),
        JointRepr::ContingencyTable { counts, total_count, .. } => (counts.clone(), *total_count),
        JointRepr::ConditionalHistograms { histograms, total_count, .. } => {
            let grid: Vec<Vec<u64>> = histograms
                .iter()
                .map(|h| h.value_count_vector())
                .collect();
            (grid, *total_count)
        }
    }
}

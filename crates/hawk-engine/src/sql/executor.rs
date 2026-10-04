use anyhow::{anyhow, Result};

use crate::query::QueryEngine;
use crate::storage::Database;

use crate::sql::formatter::QueryResult;
use crate::sql::parser::{AlertOp, DimRef, ExportFormat, Statement};

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

        Statement::Surprise {
            ref_a,
            ref_b,
            variable,
        } => exec_surprise(db, engine, ref_a, ref_b, variable.as_deref()),

        Statement::Structure { reference } => exec_structure(db, engine, reference),

        Statement::CompareStructure { ref_a, ref_b } => {
            exec_compare_structure(db, engine, ref_a, ref_b)
        }

        Statement::Estimate {
            var_a,
            var_b,
            reference,
        } => exec_estimate(
            db,
            engine,
            var_a,
            var_b,
            reference,
            Some(ESTIMATE_DISPLAY_CELLS),
        ),

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
        } => exec_show(db, engine, variable, reference, filters, *top_n, *bottom_n),

        Statement::Rank {
            variable,
            dimension,
            filters,
        } => exec_rank(db, engine, variable, dimension, filters),

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

        Statement::ExportDistribution {
            variable,
            reference,
        } => exec_export_distribution(db, engine, variable, reference),

        Statement::Alert {
            metric,
            op,
            threshold,
            variable,
            reference,
        } => exec_alert(
            db,
            engine,
            metric,
            op,
            *threshold,
            variable,
            reference.as_ref(),
        ),

        Statement::AuditStorage => exec_audit_storage(db),

        Statement::Suggest { limit } => exec_suggest(db, engine, *limit),

        Statement::Stats => exec_stats(db),
        Statement::Schema => exec_schema(db),
        Statement::Dimensions { name } => exec_dimensions(db, name.as_deref()),
    }
}

/// Build a dimension key from a primary DimRef plus optional filter DimRefs.
fn build_dim_key(reference: &DimRef, filters: &[DimRef]) -> crate::core::DimensionKey {
    let pairs = std::iter::once((reference.dimension.clone(), reference.value.clone())).chain(
        filters
            .iter()
            .map(|f| (f.dimension.clone(), f.value.clone())),
    );
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

    let result = engine.compare(db, &ref_a_str, &ref_b_str, Some(variable))?;

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
                        format!("{} vs {}", result.sample_count_a, result.sample_count_b),
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

fn exec_surprise(
    db: &Database,
    engine: &QueryEngine,
    ref_a: &DimRef,
    ref_b: &DimRef,
    variable: Option<&str>,
) -> Result<QueryResult> {
    let results = engine.surprise(db, &ref_a.to_ref_string(), &ref_b.to_ref_string(), variable)?;

    // No variable: rank all variables by excess bits.
    if variable.is_none() {
        let rows = results
            .iter()
            .map(|r| {
                vec![
                    r.variable.clone(),
                    format!("{:.4}", r.excess_bits),
                    format!("{:.4}", r.bits_per_sample),
                    format!("{:.4}", r.entropy_a),
                    format!("{:.4}", r.baseline_entropy),
                    format!("{:.4}", r.unseen_mass),
                    format!("{} vs {}", r.sample_count_a, r.sample_count_b),
                ]
            })
            .collect();
        return Ok(QueryResult {
            header: vec![
                "Variable".into(),
                "Excess Bits".into(),
                "Bits/Sample".into(),
                "H(A)".into(),
                "H(B)".into(),
                "Unseen Mass".into(),
                "Samples".into(),
            ],
            rows,
        });
    }

    let result = results
        .first()
        .ok_or_else(|| anyhow!("no surprisal result"))?;

    let mut rows = vec![
        vec![
            "Cross-Entropy H(A,B)".into(),
            format!("{:.4} bits/sample", result.bits_per_sample),
        ],
        vec!["Total Bits".into(), format!("{:.1}", result.total_bits)],
        vec![
            "Excess Bits KL(A‖B)".into(),
            format!("{:.4}", result.excess_bits),
        ],
        vec!["Entropy(A)".into(), format!("{:.4} bits", result.entropy_a)],
        vec![
            "Entropy(B)".into(),
            format!("{:.4} bits", result.baseline_entropy),
        ],
        vec![
            "Samples".into(),
            format!("{} vs {}", result.sample_count_a, result.sample_count_b),
        ],
    ];

    if let Some(warning) = &result.unseen_mass_warning {
        rows.push(vec!["Unseen Mass".into(), warning.clone()]);
    }

    if !result.top_contributors.is_empty() {
        rows.push(vec!["".into(), "".into()]);
        rows.push(vec!["--- Top Surprises ---".into(), "".into()]);
        for c in result.top_contributors.iter().take(10) {
            let marker = if c.unseen_in_b { "  [unseen in B]" } else { "" };
            rows.push(vec![
                c.label.clone(),
                format!(
                    "{:+.4} bits  ({:.3} vs {:.3}){}",
                    c.excess_bits, c.prob_a, c.prob_b, marker
                ),
            ]);
        }
    }

    Ok(QueryResult {
        header: vec!["Metric".into(), "Value".into()],
        rows,
    })
}

fn exec_structure(db: &Database, engine: &QueryEngine, reference: &DimRef) -> Result<QueryResult> {
    let result = engine.structure(db, &reference.to_ref_string())?;

    let shape = if result.is_forest() {
        format!("forest ({} components — joints missing)", result.components)
    } else {
        "tree (connected)".to_owned()
    };

    let mut rows = vec![
        vec!["Reference".into(), result.reference.clone()],
        vec!["Variables".into(), result.variables.join(", ")],
        vec![
            "Retained Information".into(),
            format!("{:.4} bits", result.retained_information),
        ],
        vec!["Shape".into(), shape],
    ];

    rows.push(vec!["".into(), "".into()]);
    rows.push(vec!["--- Edges (ranked by MI) ---".into(), "".into()]);
    if result.edges.is_empty() {
        rows.push(vec!["(none)".into(), "".into()]);
    }
    for e in &result.edges {
        rows.push(vec![
            format!("{} — {}", e.var_a, e.var_b),
            format!("{:.4} bits  (n={})", e.mi, e.sample_count),
        ]);
    }

    if !result.unknown_pairs.is_empty() {
        rows.push(vec!["".into(), "".into()]);
        rows.push(vec![
            "--- Unknown Pairs (no stored joint) ---".into(),
            "".into(),
        ]);
        for (a, b) in &result.unknown_pairs {
            rows.push(vec![format!("{} — {}", a, b), "unknown".into()]);
        }
    }

    Ok(QueryResult {
        header: vec!["Metric".into(), "Value".into()],
        rows,
    })
}

fn exec_compare_structure(
    db: &Database,
    engine: &QueryEngine,
    ref_a: &DimRef,
    ref_b: &DimRef,
) -> Result<QueryResult> {
    let diff = engine.compare_structure(db, &ref_a.to_ref_string(), &ref_b.to_ref_string())?;

    let mut rows = vec![
        vec![
            "Headline".into(),
            format!(
                "retained information changed by {:+.4} bits",
                diff.retained_information_delta
            ),
        ],
        vec![
            "Rewiring Score".into(),
            format!("{:.4}", diff.rewiring_score),
        ],
        vec![
            format!("Retained @ {}", diff.structure_a.reference),
            format!("{:.4} bits", diff.structure_a.retained_information),
        ],
        vec![
            format!("Retained @ {}", diff.structure_b.reference),
            format!("{:.4} bits", diff.structure_b.retained_information),
        ],
    ];

    if !diff.added_edges.is_empty() {
        rows.push(vec!["".into(), "".into()]);
        rows.push(vec!["--- Added Edges ---".into(), "".into()]);
        for e in &diff.added_edges {
            rows.push(vec![
                format!("{} — {}", e.var_a, e.var_b),
                format!("{:.4} bits", e.mi),
            ]);
        }
    }

    if !diff.dropped_edges.is_empty() {
        rows.push(vec!["".into(), "".into()]);
        rows.push(vec!["--- Dropped Edges ---".into(), "".into()]);
        for e in &diff.dropped_edges {
            rows.push(vec![
                format!("{} — {}", e.var_a, e.var_b),
                format!("{:.4} bits", e.mi),
            ]);
        }
    }

    if !diff.reweighted_edges.is_empty() {
        rows.push(vec!["".into(), "".into()]);
        rows.push(vec!["--- Re-weighted Edges ---".into(), "".into()]);
        for e in &diff.reweighted_edges {
            rows.push(vec![
                format!("{} — {}", e.var_a, e.var_b),
                format!("{:+.4} bits  ({:.4} → {:.4})", e.delta, e.mi_a, e.mi_b),
            ]);
        }
    }

    let unknown_a = &diff.structure_a.unknown_pairs;
    let unknown_b = &diff.structure_b.unknown_pairs;
    if !unknown_a.is_empty() || !unknown_b.is_empty() {
        rows.push(vec!["".into(), "".into()]);
        rows.push(vec![
            "--- Unknown Pairs (no stored joint) ---".into(),
            "".into(),
        ]);
        for (a, b) in unknown_a {
            rows.push(vec![
                format!("{} — {}", a, b),
                format!("unknown at {}", diff.structure_a.reference),
            ]);
        }
        for (a, b) in unknown_b {
            rows.push(vec![
                format!("{} — {}", a, b),
                format!("unknown at {}", diff.structure_b.reference),
            ]);
        }
    }

    Ok(QueryResult {
        header: vec!["Metric".into(), "Value".into()],
        rows,
    })
}

/// Interactive output shows only the most probable cells; EXPORT lifts the cap.
const ESTIMATE_DISPLAY_CELLS: usize = 20;

fn exec_estimate(
    db: &Database,
    engine: &QueryEngine,
    var_a: &str,
    var_b: &str,
    reference: &DimRef,
    display_cap: Option<usize>,
) -> Result<QueryResult> {
    let result = engine.estimate(db, var_a, var_b, &reference.to_ref_string())?;

    let banner = if result.observed {
        ">>> OBSERVED — stored joint <<<".to_owned()
    } else {
        ">>> ESTIMATED — not observed: max-entropy reconstruction from marginals <<<".to_owned()
    };

    let pad = |label: String, value: String| vec![label, value, "".into(), "".into()];
    let mut rows = vec![
        pad(banner, "".into()),
        pad(
            "Pair".into(),
            format!(
                "{} × {} at {}",
                result.var_a, result.var_b, result.reference
            ),
        ),
        pad(
            "Missing Information".into(),
            format!(
                "{:.4} bits (max MI the marginals permit)",
                result.missing_information_bits
            ),
        ),
        vec![
            "MI".into(),
            format!("{:.4} bits", result.mi),
            "0.0000".into(),
            format!("{:.4}", result.mi_upper_bound),
        ],
        pad(
            "Joint Entropy".into(),
            format!("{:.4} bits", result.joint_entropy),
        ),
        pad(
            format!("Entropy({})", result.var_a),
            format!("{:.4} bits", result.entropy_a),
        ),
        pad(
            format!("Entropy({})", result.var_b),
            format!("{:.4} bits", result.entropy_b),
        ),
        pad(
            "Samples".into(),
            format!("{} vs {}", result.sample_count_a, result.sample_count_b),
        ),
    ];
    if !result.observed {
        rows.push(pad(
            "IPF".into(),
            if result.ipf_converged {
                format!("converged in {} sweep(s)", result.ipf_iterations)
            } else {
                format!("NOT converged after {} sweep(s)", result.ipf_iterations)
            },
        ));
    }

    let shown = display_cap
        .unwrap_or(result.cells.len())
        .min(result.cells.len());
    rows.push(pad("".into(), "".into()));
    rows.push(pad(
        format!(
            "--- Cells ({} of {} by probability) ---",
            shown,
            result.cells.len()
        ),
        if shown < result.cells.len() {
            "use EXPORT ... AS JSON|CSV for the full grid".into()
        } else {
            "".into()
        },
    ));
    for c in result.cells.iter().take(shown) {
        rows.push(vec![
            format!("{} × {}", c.label_a, c.label_b),
            format!("{:.6}", c.probability),
            format!("{:.6}", c.lower_bound),
            format!("{:.6}", c.upper_bound),
        ]);
    }

    Ok(QueryResult {
        header: vec![
            "Metric / Cell".into(),
            "Value".into(),
            "Frechet Lower".into(),
            "Frechet Upper".into(),
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
    let result = engine.track(db, &reference.to_ref_string(), None, None, granularity)?;

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
    engine: &QueryEngine,
    variable: &str,
    reference: &DimRef,
    filters: &[DimRef],
    top_n: Option<usize>,
    bottom_n: Option<usize>,
) -> Result<QueryResult> {
    let dim_key = build_dim_key(reference, filters);
    let mut dist = db
        .get_distribution(variable, &dim_key)
        .ok_or_else(|| anyhow!("distribution not found"))?
        .clone();
    if let Some(k) = engine.min_cell_count() {
        dist.suppress_small_cells(k);
    }

    let mut rows = vec![
        vec!["Entropy".into(), format!("{:.4} bits", dist.entropy)],
        vec!["Samples".into(), format!("{}", dist.sample_count)],
        vec!["Version".into(), format!("{}", dist.version)],
        vec!["".into(), "".into()],
    ];

    // Collect category/bin rows with their probability for sorting
    let mut cat_rows: Vec<(f64, Vec<String>)> = Vec::new();

    match &dist.repr {
        crate::core::DistributionRepr::Categorical { total_count, .. } => {
            let categories = dist
                .repr
                .categorical_labels_with_unknown()
                .expect("categorical labels expected");
            let counts = dist.repr.value_count_vector();
            for (cat, count) in categories.iter().zip(counts.iter()) {
                let prob = if *total_count > 0 {
                    *count as f64 / *total_count as f64
                } else {
                    0.0
                };
                let bar = "#".repeat((prob * 40.0) as usize);
                cat_rows.push((
                    prob,
                    vec![cat.clone(), format!("{:6}  {:.4}  {}", count, prob, bar)],
                ));
            }
        }
        crate::core::DistributionRepr::Histogram {
            min,
            max,
            bin_counts,
            total_count,
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
    engine: &QueryEngine,
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
            // Released entropy matches the suppressed distribution.
            let entropy = match engine.min_cell_count() {
                Some(k) => {
                    let mut released = d.clone();
                    released.suppress_small_cells(k);
                    released.entropy
                }
                None => d.entropy,
            };
            d.dimension_key
                .get(dimension)
                .map(|v| (v.clone(), entropy, d.sample_count))
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
    let mi = match engine.mutual_info(db, var_a, var_b, &reference.to_ref_string()) {
        Ok(mi) => mi,
        // Planner fallback: no stored joint → report the max-ent estimate
        // with an explicit warning instead of erroring (or silently zeroing).
        Err(err) if engine.mi_estimate_fallback() => {
            return exec_mi_estimate_fallback(db, engine, var_a, var_b, reference, err)
        }
        Err(err) => return Err(err),
    };

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

fn exec_mi_estimate_fallback(
    db: &Database,
    engine: &QueryEngine,
    var_a: &str,
    var_b: &str,
    reference: &DimRef,
    mi_err: anyhow::Error,
) -> Result<QueryResult> {
    let est = engine
        .estimate(db, var_a, var_b, &reference.to_ref_string())
        .map_err(|est_err| anyhow!("{}; estimate fallback also failed: {}", mi_err, est_err))?;

    let mut rows = vec![vec!["MI".into(), format!("{:.4} bits", est.mi)]];
    if est.observed {
        // Aggregated stored joints matched the slice even though the exact
        // key lookup failed.
        rows.push(vec![
            "Source".into(),
            "OBSERVED — aggregated from stored joints".into(),
        ]);
    } else {
        rows.push(vec!["Estimated".into(), "true".into()]);
        rows.push(vec![
            "Warning".into(),
            format!(
                "estimate from marginals — MI lower bound 0; true MI ≤ {:.4} bits",
                est.mi_upper_bound
            ),
        ]);
        rows.push(vec![
            "Missing Information".into(),
            format!("{:.4} bits", est.missing_information_bits),
        ]);
    }
    rows.push(vec![
        "Samples".into(),
        format!("{} vs {}", est.sample_count_a, est.sample_count_b),
    ]);

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
            for (j, value) in matrix[i].iter().enumerate().take(labels.len()) {
                row.push(if i == j {
                    "—".into()
                } else {
                    format!("{:.4}", value)
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
    // ESTIMATE caps its interactive cell listing; exports carry the full grid.
    let inner_result = match inner {
        Statement::Estimate {
            var_a,
            var_b,
            reference,
        } => exec_estimate(db, engine, var_a, var_b, reference, None)?,
        _ => execute(db, engine, inner)?,
    };

    let output = match format {
        ExportFormat::Csv => inner_result.to_csv(),
        ExportFormat::Json => inner_result.to_json(),
    };

    Ok(QueryResult {
        header: vec!["Output".into()],
        rows: vec![vec![output]],
    })
}

fn check_threshold(value: f64, op: &AlertOp, threshold: f64) -> bool {
    match op {
        AlertOp::Gt => value > threshold,
        AlertOp::Lt => value < threshold,
        AlertOp::Gte => value >= threshold,
        AlertOp::Lte => value <= threshold,
    }
}

fn exec_alert(
    db: &Database,
    engine: &QueryEngine,
    metric: &str,
    op: &AlertOp,
    threshold: f64,
    variable: &str,
    reference: Option<&DimRef>,
) -> Result<QueryResult> {
    // Determine the dimension to track from
    let ref_str = match reference {
        Some(r) => r.to_ref_string(),
        None => {
            let dims = db.schema().dimensions.clone();
            let first_dim = dims
                .first()
                .ok_or_else(|| anyhow!("no dimensions defined; use FROM <dim:val>"))?;
            let first_val = db
                .dimension_values(&first_dim.name)
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("dimension '{}' has no values", first_dim.name))?;
            format!("{}:{}", first_dim.name, first_val)
        }
    };

    let track = engine.track(db, &ref_str, None, None, None)?;

    let metric_lower = metric.to_ascii_lowercase();
    let op_str = match op {
        AlertOp::Gt => ">",
        AlertOp::Lt => "<",
        AlertOp::Gte => ">=",
        AlertOp::Lte => "<=",
    };

    let mut rows = Vec::new();

    for i in 0..track.time_points.len().saturating_sub(1) {
        let value = match metric_lower.as_str() {
            "jsd" => track.drift_series.get(i).copied().unwrap_or(0.0),
            "entropy" => track.entropy_series[i],
            "psi" | "hellinger" => {
                // Need to run compare for these metrics
                let ref_a = format!(
                    "{}:{}",
                    reference.map(|r| r.dimension.as_str()).unwrap_or(
                        db.schema()
                            .dimensions
                            .first()
                            .map(|d| d.name.as_str())
                            .unwrap_or("")
                    ),
                    track.time_points[i]
                );
                let ref_b = format!(
                    "{}:{}",
                    reference.map(|r| r.dimension.as_str()).unwrap_or(
                        db.schema()
                            .dimensions
                            .first()
                            .map(|d| d.name.as_str())
                            .unwrap_or("")
                    ),
                    track.time_points[i + 1]
                );
                match engine.compare(db, &ref_a, &ref_b, Some(variable)) {
                    Ok(cmp) => {
                        if metric_lower == "psi" {
                            cmp.psi
                        } else {
                            cmp.hellinger
                        }
                    }
                    Err(_) => continue,
                }
            }
            "surprisal" => {
                // Excess bits of the newer slice's data under the previous slice's model.
                let dim = reference.map(|r| r.dimension.as_str()).unwrap_or(
                    db.schema()
                        .dimensions
                        .first()
                        .map(|d| d.name.as_str())
                        .unwrap_or(""),
                );
                let ref_new = format!("{}:{}", dim, track.time_points[i + 1]);
                let ref_model = format!("{}:{}", dim, track.time_points[i]);
                match engine.surprise(db, &ref_new, &ref_model, Some(variable)) {
                    Ok(results) => match results.first() {
                        Some(r) => r.excess_bits,
                        None => continue,
                    },
                    Err(_) => continue,
                }
            }
            other => {
                return Err(anyhow!(
                    "unsupported alert metric '{}'; use jsd, psi, hellinger, entropy, or surprisal",
                    other
                ))
            }
        };

        if check_threshold(value, op, threshold) {
            rows.push(vec![
                track.time_points[i].clone(),
                track.time_points[i + 1].clone(),
                format!("{:.6}", value),
                format!("{} {} {}", metric_lower, op_str, threshold),
            ]);
        }
    }

    if rows.is_empty() {
        rows.push(vec![
            "No alerts triggered".into(),
            "".into(),
            "".into(),
            format!("{} {} {}", metric_lower, op_str, threshold),
        ]);
    }

    Ok(QueryResult {
        header: vec![
            "Time From".into(),
            "Time To".into(),
            metric.to_uppercase(),
            "Condition".into(),
        ],
        rows,
    })
}

fn exec_export_distribution(
    db: &Database,
    engine: &QueryEngine,
    variable: &str,
    reference: &DimRef,
) -> Result<QueryResult> {
    let dim_key = build_dim_key(reference, &[]);
    let mut dist = db
        .get_distribution(variable, &dim_key)
        .ok_or_else(|| {
            anyhow!(
                "distribution not found for '{}' at {}",
                variable,
                reference.to_ref_string()
            )
        })?
        .clone();
    if let Some(k) = engine.min_cell_count() {
        dist.suppress_small_cells(k);
    }

    let json = serde_json::to_string_pretty(&dist.repr)
        .map_err(|e| anyhow!("failed to serialize distribution: {}", e))?;

    Ok(QueryResult {
        header: vec!["Distribution JSON".into()],
        rows: vec![vec![json]],
    })
}

/// AUDIT STORAGE — advisory MDL report per stored object: approximate
/// on-disk cost (bincode serialize-and-measure), information retained in
/// bits, and a recommendation. Mutates nothing.
fn exec_audit_storage(db: &Database) -> Result<QueryResult> {
    use crate::math::{category_fold_candidates, choose_bin_count, score_histogram_bins, score_joint};
    use crate::storage::DEFAULT_SNAPSHOT_EPSILON_BITS;

    let mut rows = Vec::new();
    let mut total_bytes: u64 = 0;
    let mut candidate_savings: u64 = 0;

    for var in &db.schema().variables {
        for dist in db.distributions_for_variable(&var.name) {
            let bytes = bincode::serialized_size(dist).unwrap_or(0);
            total_bytes += bytes;
            let label = format!("{} @ {}", dist.variable, key_label(&dist.dimension_key));
            let info = format!("{:.4} bits/sample entropy", dist.entropy);

            let recommendation = match &dist.repr {
                crate::core::DistributionRepr::Histogram { bin_counts, .. } => {
                    let current = bin_counts.len();
                    let scores = score_histogram_bins(&dist.repr, &halving_candidates(current))
                        .map_err(|e| anyhow!(e.to_string()))?;
                    let best = choose_bin_count(&scores).unwrap_or(current);
                    if best < current {
                        let saved = (current - best) as u64 * 8;
                        let lost = scores
                            .iter()
                            .find(|s| s.bins == best)
                            .map(|s| dist.entropy - s.entropy_bits)
                            .unwrap_or(0.0)
                            .max(0.0);
                        candidate_savings += saved;
                        if lost < 0.01 {
                            format!(
                                "rebin {} → {} bins: saves ~{}, loses {:.4} bits/sample",
                                current,
                                best,
                                format_bytes(saved),
                                lost
                            )
                        } else {
                            // The resolution loses real entropy when
                            // coarsened, but the sample count is too small
                            // for the retained bits to pay for the bins.
                            format!(
                                "over-resolved for {} samples: MDL-optimal {} bins saves ~{} (rebinning loses {:.4} bits/sample)",
                                dist.sample_count,
                                best,
                                format_bytes(saved),
                                lost
                            )
                        }
                    } else {
                        "resolution earns its bytes".to_owned()
                    }
                }
                crate::core::DistributionRepr::Categorical { categories, .. } => {
                    let folds = category_fold_candidates(&dist.repr);
                    if folds.is_empty() {
                        "every category earns its bytes".to_owned()
                    } else {
                        let saved =
                            (folds.iter().map(|f| f.storage_bits).sum::<f64>() / 8.0) as u64;
                        candidate_savings += saved;
                        let preview = folds
                            .iter()
                            .take(3)
                            .map(|f| f.category.as_str())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!(
                            "{} of {} categories carry ~0 bits ({}) — fold candidates, saves ~{}",
                            folds.len(),
                            categories.len(),
                            preview,
                            format_bytes(saved)
                        )
                    }
                }
            };
            rows.push(vec![label, format_bytes(bytes), info, recommendation]);
        }
    }

    let mut seen_joints = std::collections::HashSet::new();
    for (var_a, var_b) in &db.schema().joints {
        for joint in db.joints_for_pair(var_a, var_b) {
            if !seen_joints.insert(joint.id) {
                continue;
            }
            let bytes = bincode::serialized_size(joint).unwrap_or(0);
            total_bytes += bytes;
            let (counts, total) = extract_joint_counts(joint);
            let score = score_joint(&counts, total, bytes);
            let label = format!(
                "joint {}×{} @ {}",
                joint.variables.0,
                joint.variables.1,
                key_label(&joint.dimension_key)
            );
            let info = format!(
                "{:.4} bits/sample MI ({:.1} bits total)",
                score.mi_bits_per_sample, score.dependency_bits
            );
            let recommendation = if score.worth_keeping {
                "earns its bytes".to_owned()
            } else {
                candidate_savings += bytes;
                format!(
                    "candidate to drop; ESTIMATE would recover it within {:.4} bits/sample",
                    score.estimate_gap_bits
                )
            };
            rows.push(vec![label, format_bytes(bytes), info, recommendation]);
        }
    }

    let snap = db.audit_snapshots(DEFAULT_SNAPSHOT_EPSILON_BITS);
    total_bytes += snap.serialized_bytes;
    if snap.entries > 0 && snap.redundant > 0 {
        candidate_savings += snap.serialized_bytes * snap.redundant as u64 / snap.entries as u64;
    }
    rows.push(vec![
        "snapshots".into(),
        format_bytes(snap.serialized_bytes),
        format!("{} entries", snap.entries),
        format!(
            "{} snapshots redundant at eps={} bits — compact_snapshots({}) reclaims them",
            snap.redundant, DEFAULT_SNAPSHOT_EPSILON_BITS, DEFAULT_SNAPSHOT_EPSILON_BITS
        ),
    ]);

    rows.push(vec!["".into(), "".into(), "".into(), "".into()]);
    rows.push(vec![
        "Total size".into(),
        format_bytes(total_bytes),
        "".into(),
        "".into(),
    ]);
    rows.push(vec![
        "Candidate savings".into(),
        format_bytes(candidate_savings),
        "".into(),
        "advisory only — nothing was modified".into(),
    ]);

    Ok(QueryResult {
        header: vec![
            "Object".into(),
            "Size".into(),
            "Information".into(),
            "Recommendation".into(),
        ],
        rows,
    })
}

/// Coarser candidate resolutions for a stored histogram: successive halvings.
fn halving_candidates(current: usize) -> Vec<usize> {
    let mut candidates = Vec::new();
    let mut k = current / 2;
    while k >= 1 {
        candidates.push(k);
        if k == 1 {
            break;
        }
        k /= 2;
    }
    candidates
}

fn key_label(key: &crate::core::DimensionKey) -> String {
    let label = crate::core::canonical_dimension_key(key);
    if label.is_empty() {
        "(all)".to_owned()
    } else {
        label
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

/// SUGGEST — ranked next queries by expected information gain. The DSL path
/// carries no session history; the MCP `suggest` tool adds ledger dedup.
fn exec_suggest(db: &Database, engine: &QueryEngine, limit: usize) -> Result<QueryResult> {
    let suggestions = engine.suggest(db, &crate::query::suggest::SuggestContext::default(), limit)?;

    let mut rows: Vec<Vec<String>> = suggestions
        .iter()
        .map(|s| {
            vec![
                s.query.clone(),
                format!("{:.4}", s.expected_bits),
                s.rationale.clone(),
            ]
        })
        .collect();
    if rows.is_empty() {
        rows.push(vec![
            "(nothing to suggest — no scorable candidates)".into(),
            "".into(),
            "".into(),
        ]);
    }

    Ok(QueryResult {
        header: vec![
            "Suggested Query".into(),
            "Expected Bits".into(),
            "Rationale".into(),
        ],
        rows,
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
                let r = range
                    .map(|(a, b)| format!("[{}, {}]", a, b))
                    .unwrap_or_default();
                format!("continuous  bins={}  range={}", bins, r)
            }
            crate::core::VariableType::Categorical {
                categories,
                allow_unknown,
            } => {
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
        JointRepr::HistogramGrid {
            counts,
            total_count,
            ..
        } => (counts.clone(), *total_count),
        JointRepr::ContingencyTable {
            counts,
            total_count,
            ..
        } => (counts.clone(), *total_count),
        JointRepr::ConditionalHistograms {
            histograms,
            total_count,
            ..
        } => {
            let grid: Vec<Vec<u64>> = histograms.iter().map(|h| h.value_count_vector()).collect();
            (grid, *total_count)
        }
    }
}

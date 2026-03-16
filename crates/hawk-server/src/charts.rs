use hawk_query::QueryEngine;
use hawk_storage::Database;

/// Generate an SVG chart if the query type benefits from visualization.
pub fn maybe_chart(query: &str, db: &Database, engine: &QueryEngine) -> String {
    let upper = query.to_ascii_uppercase();

    if upper.starts_with("COMPARE") {
        return compare_chart(query, db, engine);
    }
    if upper.starts_with("PAIRWISE") {
        return pairwise_chart(query, db, engine);
    }
    if upper.starts_with("TRACK") {
        return track_chart(query, db, engine);
    }
    if upper.starts_with("RANK") {
        return rank_chart(query, db);
    }
    if upper.starts_with("SHOW") {
        return show_chart(query, db);
    }

    String::new()
}

/// Entropy timeline SVG for the overview page.
pub fn entropy_timeline_svg(data: &[(String, f64, u64)], var: &str, dim: &str) -> String {
    if data.is_empty() {
        return String::new();
    }

    let w = 680.0_f64;
    let h = 200.0_f64;
    let pad_l = 50.0;
    let pad_r = 20.0;
    let pad_t = 30.0;
    let pad_b = 40.0;
    let plot_w = w - pad_l - pad_r;
    let plot_h = h - pad_t - pad_b;

    let max_ent = data.iter().map(|(_, e, _)| *e).fold(0.0_f64, f64::max) * 1.1;
    let min_ent = data.iter().map(|(_, e, _)| *e).fold(f64::MAX, f64::min) * 0.9;
    let range = (max_ent - min_ent).max(0.1);

    let n = data.len();
    let points: Vec<(f64, f64)> = data
        .iter()
        .enumerate()
        .map(|(i, (_, ent, _))| {
            let x = pad_l + (i as f64 / (n - 1).max(1) as f64) * plot_w;
            let y = pad_t + (1.0 - (ent - min_ent) / range) * plot_h;
            (x, y)
        })
        .collect();

    let polyline: String = points
        .iter()
        .map(|(x, y)| format!("{:.1},{:.1}", x, y))
        .collect::<Vec<_>>()
        .join(" ");

    let dots: String = points
        .iter()
        .enumerate()
        .map(|(i, (x, y))| {
            format!(
                "<circle cx=\"{:.1}\" cy=\"{:.1}\" r=\"4\" fill=\"#58a6ff\"><title>{}: H={:.3}</title></circle>",
                x, y, data[i].0, data[i].1
            )
        })
        .collect();

    let x_labels: String = data
        .iter()
        .enumerate()
        .map(|(i, (label, _, _))| {
            let x = pad_l + (i as f64 / (n - 1).max(1) as f64) * plot_w;
            format!(
                "<text x=\"{:.1}\" y=\"{:.0}\" text-anchor=\"middle\" fill=\"#8b949e\" font-size=\"11\">{}</text>",
                x, h - 8.0, label
            )
        })
        .collect();

    format!(
        r##"<div style="margin:0.75rem 0"><div style="color:#8b949e;font-size:0.8rem;margin-bottom:0.25rem">{var} entropy over {dim}</div>
<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">
  <rect x="{pl}" y="{pt}" width="{pw}" height="{ph}" fill="#161b22" rx="4"/>
  <polyline points="{polyline}" fill="none" stroke="#58a6ff" stroke-width="2" stroke-linejoin="round"/>
  {dots}
  {x_labels}
  <text x="10" y="{yt}" fill="#8b949e" font-size="11">{max:.1}</text>
  <text x="10" y="{yb}" fill="#8b949e" font-size="11">{min:.1}</text>
</svg></div>"##,
        var = var,
        dim = dim,
        w = w as u32,
        h = h as u32,
        pl = pad_l,
        pt = pad_t,
        pw = plot_w,
        ph = plot_h,
        polyline = polyline,
        dots = dots,
        x_labels = x_labels,
        yt = pad_t + 4.0,
        yb = pad_t + plot_h + 4.0,
        max = max_ent,
        min = min_ent,
    )
}

fn pairwise_chart(query: &str, db: &Database, engine: &QueryEngine) -> String {
    let stmt = match hawk_sql::parser::parse(query) {
        Ok(s) => s,
        Err(_) => return String::new(),
    };

    let hawk_sql::parser::Statement::Pairwise { dimension, variable, metric } = stmt else {
        return String::new();
    };

    let (labels, matrix) = match engine.pairwise(db, &dimension, &variable, &metric) {
        Ok(r) => r,
        Err(_) => return String::new(),
    };

    pairwise_heatmap_svg(&labels, &matrix)
}

/// Render a color-coded SVG heatmap for PAIRWISE query results.
/// Uses a blue gradient where darker blue = higher divergence.
pub fn pairwise_heatmap_svg(labels: &[String], matrix: &[Vec<f64>]) -> String {
    let n = labels.len();
    if n == 0 {
        return String::new();
    }

    let cell_size = 36.0_f64;
    let label_w = 80.0_f64;
    let label_h = 80.0_f64;
    let w = label_w + n as f64 * cell_size + 20.0;
    let h = label_h + n as f64 * cell_size + 20.0;

    // Find max value for color scaling
    let max_val = matrix
        .iter()
        .flat_map(|row| row.iter())
        .copied()
        .fold(0.0_f64, f64::max)
        .max(0.001);

    let mut cells = String::new();

    for i in 0..n {
        for j in 0..n {
            let val = matrix[i][j];
            let t = (val / max_val).min(1.0);
            // Blue gradient: from #0d1117 (dark bg) to #1f6feb (bright blue)
            let r = (13.0 + t * (31.0 - 13.0)) as u8;
            let g = (17.0 + t * (111.0 - 17.0)) as u8;
            let b = (23.0 + t * (235.0 - 23.0)) as u8;
            let x = label_w + j as f64 * cell_size;
            let y = label_h + i as f64 * cell_size;
            cells.push_str(&format!(
                r##"<rect x="{x:.0}" y="{y:.0}" width="{cs}" height="{cs}" fill="#{r:02x}{g:02x}{b:02x}" stroke="#30363d" stroke-width="0.5"><title>{li} vs {lj}: {val:.4}</title></rect>"##,
                x = x,
                y = y,
                cs = cell_size,
                r = r,
                g = g,
                b = b,
                li = labels[i],
                lj = labels[j],
                val = val,
            ));
        }
    }

    // Column labels (top, rotated)
    let col_labels: String = labels
        .iter()
        .enumerate()
        .map(|(j, label)| {
            let x = label_w + j as f64 * cell_size + cell_size / 2.0;
            let y = label_h - 4.0;
            format!(
                r##"<text x="{x:.0}" y="{y:.0}" fill="#c9d1d9" font-size="10" text-anchor="end" transform="rotate(-45 {x:.0} {y:.0})">{label}</text>"##,
                x = x,
                y = y,
                label = if label.len() > 8 { &label[..8] } else { label },
            )
        })
        .collect();

    // Row labels (left)
    let row_labels: String = labels
        .iter()
        .enumerate()
        .map(|(i, label)| {
            let x = label_w - 4.0;
            let y = label_h + i as f64 * cell_size + cell_size / 2.0 + 4.0;
            format!(
                r##"<text x="{x:.0}" y="{y:.0}" fill="#c9d1d9" font-size="10" text-anchor="end">{label}</text>"##,
                x = x,
                y = y,
                label = if label.len() > 8 { &label[..8] } else { label },
            )
        })
        .collect();

    format!(
        r##"<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">
  {cells}
  {col_labels}
  {row_labels}
</svg>"##,
        w = w as u32,
        h = h as u32,
        cells = cells,
        col_labels = col_labels,
        row_labels = row_labels,
    )
}

fn compare_chart(query: &str, db: &Database, engine: &QueryEngine) -> String {
    let stmt = match hawk_sql::parser::parse(query) {
        Ok(s) => s,
        Err(_) => return String::new(),
    };

    let hawk_sql::parser::Statement::Compare { variable, ref_a, ref_b, .. } = stmt else {
        return String::new();
    };

    let result = match engine.compare(
        db,
        &ref_a.to_ref_string(),
        &ref_b.to_ref_string(),
        Some(&variable),
    ) {
        Ok(r) => r,
        Err(_) => return String::new(),
    };

    if result.top_movers.is_empty() {
        return String::new();
    }

    // --- Side-by-side grouped bar chart of prob_a vs prob_b (top 12) ---
    let movers_overlay = &result.top_movers[..result.top_movers.len().min(12)];
    let overlay_svg = {
        let bar_h = 10.0_f64;
        let gap = 6.0_f64;
        let pair_h = bar_h * 2.0 + 2.0; // two bars stacked closely
        let label_w = 140.0_f64;
        let chart_w = 550.0_f64;
        let bar_area = chart_w - label_w - 60.0;
        let total_h = movers_overlay.len() as f64 * (pair_h + gap) + 30.0;

        let max_prob = movers_overlay
            .iter()
            .map(|m| m.prob_a.max(m.prob_b))
            .fold(0.0_f64, f64::max)
            .max(0.01);

        let bars: String = movers_overlay
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let y_base = 25.0 + i as f64 * (pair_h + gap);
                let w_a = (m.prob_a / max_prob) * bar_area;
                let w_b = (m.prob_b / max_prob) * bar_area;
                format!(
                    r##"<text x="{lx}" y="{ty}" fill="#c9d1d9" font-size="11" text-anchor="end">{cat}</text><rect x="{bx}" y="{ya:.0}" width="{wa:.1}" height="{bh}" fill="#58a6ff" rx="2" opacity="0.8"/><text x="{va:.1}" y="{tya}" fill="#8b949e" font-size="9">{pa:.1}%</text><rect x="{bx}" y="{yb:.0}" width="{wb:.1}" height="{bh}" fill="#f0883e" rx="2" opacity="0.8"/><text x="{vb:.1}" y="{tyb}" fill="#8b949e" font-size="9">{pb:.1}%</text>"##,
                    lx = label_w - 5.0,
                    ty = y_base + pair_h / 2.0 + 3.0,
                    cat = m.category,
                    bx = label_w,
                    ya = y_base,
                    wa = w_a,
                    bh = bar_h,
                    va = label_w + w_a + 3.0,
                    tya = y_base + bar_h - 1.0,
                    pa = m.prob_a * 100.0,
                    yb = y_base + bar_h + 2.0,
                    wb = w_b,
                    vb = label_w + w_b + 3.0,
                    tyb = y_base + bar_h * 2.0 + 1.0,
                    pb = m.prob_b * 100.0,
                )
            })
            .collect();

        format!(
            r##"<div style="margin-bottom:0.5rem"><div style="color:#8b949e;font-size:0.8rem;margin-bottom:0.25rem">distribution overlay &mdash; <span style="color:#58a6ff">A (blue)</span> vs <span style="color:#f0883e">B (orange)</span></div><svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">{bars}</svg></div>"##,
            w = chart_w as u32,
            h = total_h as u32,
            bars = bars,
        )
    };

    // Horizontal diverging bar chart of top movers
    let movers = &result.top_movers[..result.top_movers.len().min(12)];
    let max_delta = movers.iter().map(|m| m.delta.abs()).fold(0.0_f64, f64::max).max(0.01);

    let bar_h = 22.0;
    let gap = 4.0;
    let label_w = 140.0;
    let chart_w = 500.0;
    let mid = label_w + (chart_w - label_w) / 2.0;
    let bar_area = (chart_w - label_w) / 2.0;
    let total_h = (movers.len() as f64) * (bar_h + gap) + 30.0;

    let bars: String = movers
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let y = 25.0 + i as f64 * (bar_h + gap);
            let bar_w = (m.delta.abs() / max_delta) * bar_area;
            let (x, color) = if m.delta >= 0.0 {
                (mid, "#7ee787")
            } else {
                (mid - bar_w, "#f85149")
            };
            format!(
                "<text x=\"{lx}\" y=\"{ty}\" fill=\"#c9d1d9\" font-size=\"12\" text-anchor=\"end\">{cat}</text>\
                 <rect x=\"{x:.1}\" y=\"{y:.0}\" width=\"{w:.1}\" height=\"{h}\" fill=\"{color}\" rx=\"2\" opacity=\"0.8\"/>\
                 <text x=\"{vx:.1}\" y=\"{ty}\" fill=\"#8b949e\" font-size=\"10\">{delta:+.3}</text>",
                lx = label_w - 5.0,
                ty = y + bar_h * 0.7,
                cat = m.category,
                x = x,
                y = y,
                w = bar_w,
                h = bar_h,
                color = color,
                vx = if m.delta >= 0.0 { mid + bar_w + 4.0 } else { mid - bar_w - 4.0 },
                delta = m.delta,
            )
        })
        .collect();

    let diverging_svg = format!(
        r##"<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">
  <line x1="{mid}" y1="20" x2="{mid}" y2="{bot}" stroke="#30363d" stroke-width="1"/>
  <text x="{mid}" y="14" text-anchor="middle" fill="#8b949e" font-size="11">probability shift</text>
  {bars}
</svg>"##,
        w = chart_w as u32,
        h = total_h as u32,
        mid = mid,
        bot = total_h - 5.0,
        bars = bars,
    );

    format!("{}{}", overlay_svg, diverging_svg)
}

fn track_chart(query: &str, db: &Database, engine: &QueryEngine) -> String {
    let stmt = match hawk_sql::parser::parse(query) {
        Ok(s) => s,
        Err(_) => return String::new(),
    };

    let hawk_sql::parser::Statement::Track { reference, granularity, .. } = &stmt else {
        return String::new();
    };

    let result = match engine.track(db, &reference.to_ref_string(), None, None, granularity.as_deref()) {
        Ok(r) => r,
        Err(_) => return String::new(),
    };

    if result.time_points.is_empty() {
        return String::new();
    }

    let data: Vec<(String, f64, u64)> = result
        .time_points
        .iter()
        .enumerate()
        .map(|(i, tp)| {
            let sample = result.snapshots.get(i).map(|s| s.sample_count).unwrap_or(0);
            (tp.clone(), result.entropy_series[i], sample)
        })
        .collect();

    entropy_timeline_svg(&data, "distribution", "time")
}

fn rank_chart(query: &str, db: &Database) -> String {
    let stmt = match hawk_sql::parser::parse(query) {
        Ok(s) => s,
        Err(_) => return String::new(),
    };

    let hawk_sql::parser::Statement::Rank { variable, dimension, .. } = &stmt else {
        return String::new();
    };

    let mut ranked: Vec<_> = db
        .distributions_for_variable(variable)
        .into_iter()
        .filter_map(|d| {
            d.dimension_key
                .get(dimension)
                .map(|v| (v.clone(), d.entropy, d.sample_count))
        })
        .collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));

    if ranked.is_empty() {
        return String::new();
    }

    // Horizontal bar chart
    let max_ent = ranked.iter().map(|(_, e, _)| *e).fold(0.0_f64, f64::max).max(0.1);
    let bar_h = 22.0;
    let gap = 4.0;
    let label_w = 70.0;
    let chart_w = 500.0;
    let total_h = ranked.len() as f64 * (bar_h + gap) + 10.0;

    let bars: String = ranked
        .iter()
        .enumerate()
        .map(|(i, (label, ent, _))| {
            let y = 5.0 + i as f64 * (bar_h + gap);
            let w = (ent / max_ent) * (chart_w - label_w - 60.0);
            format!(
                "<text x=\"{lx}\" y=\"{ty}\" fill=\"#c9d1d9\" font-size=\"12\" text-anchor=\"end\">{label}</text>\
                 <rect x=\"{bx}\" y=\"{y:.0}\" width=\"{w:.1}\" height=\"{h}\" fill=\"#58a6ff\" rx=\"2\" opacity=\"0.7\"/>\
                 <text x=\"{vx:.1}\" y=\"{ty}\" fill=\"#8b949e\" font-size=\"11\">{ent:.3}</text>",
                lx = label_w - 5.0,
                ty = y + bar_h * 0.7,
                label = label,
                bx = label_w,
                y = y,
                w = w,
                h = bar_h,
                vx = label_w + w + 5.0,
                ent = ent,
            )
        })
        .collect();

    format!(
        r##"<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">{bars}</svg>"##,
        w = chart_w as u32,
        h = total_h as u32,
        bars = bars,
    )
}

fn show_chart(query: &str, db: &Database) -> String {
    let stmt = match hawk_sql::parser::parse(query) {
        Ok(s) => s,
        Err(_) => return String::new(),
    };

    let hawk_sql::parser::Statement::Show { variable, reference, .. } = &stmt else {
        return String::new();
    };

    let dim_key = hawk_core::dimension_key_from_pairs(std::iter::once((
        reference.dimension.clone(),
        reference.value.clone(),
    )));
    let dist = match db.get_distribution(&variable, &dim_key) {
        Some(d) => d,
        None => return String::new(),
    };

    match &dist.repr {
        hawk_core::DistributionRepr::Categorical { categories, counts, total_count, .. } => {
            if *total_count == 0 || categories.is_empty() {
                return String::new();
            }
            // Top 15 categories horizontal bar chart
            let mut pairs: Vec<(&str, f64)> = categories
                .iter()
                .zip(counts.iter())
                .map(|(c, n)| (c.as_str(), *n as f64 / *total_count as f64))
                .collect();
            pairs.sort_by(|a, b| b.1.total_cmp(&a.1));
            pairs.truncate(15);

            let max_p = pairs.iter().map(|(_, p)| *p).fold(0.0_f64, f64::max).max(0.01);
            let bar_h = 20.0;
            let gap = 3.0;
            let label_w = 130.0;
            let chart_w = 550.0;
            let total_h = pairs.len() as f64 * (bar_h + gap) + 10.0;

            let bars: String = pairs
                .iter()
                .enumerate()
                .map(|(i, (label, prob))| {
                    let y = 5.0 + i as f64 * (bar_h + gap);
                    let w = (prob / max_p) * (chart_w - label_w - 80.0);
                    format!(
                        "<text x=\"{lx}\" y=\"{ty}\" fill=\"#c9d1d9\" font-size=\"11\" text-anchor=\"end\">{label}</text>\
                         <rect x=\"{bx}\" y=\"{y:.0}\" width=\"{w:.1}\" height=\"{h}\" fill=\"#58a6ff\" rx=\"2\" opacity=\"0.7\"/>\
                         <text x=\"{vx:.1}\" y=\"{ty}\" fill=\"#8b949e\" font-size=\"10\">{pct:.1}%</text>",
                        lx = label_w - 5.0,
                        ty = y + bar_h * 0.65,
                        label = label,
                        bx = label_w,
                        y = y,
                        w = w,
                        h = bar_h,
                        vx = label_w + w + 5.0,
                        pct = prob * 100.0,
                    )
                })
                .collect();

            format!(
                r##"<svg viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">{bars}</svg>"##,
                w = chart_w as u32,
                h = total_h as u32,
                bars = bars,
            )
        }
        _ => String::new(),
    }
}

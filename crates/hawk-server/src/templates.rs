use hawk_engine::core::Schema;
use hawk_engine::sql::formatter::QueryResult;
use hawk_engine::storage::DatabaseStats;

pub fn index_page(db_path: &str, stats: &DatabaseStats, schema: &Schema) -> String {
    let first_dim_name = schema.dimensions.first().map(|d| d.name.as_str()).unwrap_or("time");
    let first_dim_value = "1";

    let variables_html: String = schema
        .variables
        .iter()
        .map(|v| {
            let detail = match &v.var_type {
                hawk_engine::core::VariableType::Continuous { bins, range } => {
                    let r = range
                        .map(|(a, b)| format!("[{}, {}]", a, b))
                        .unwrap_or_default();
                    format!("continuous &middot; {} bins &middot; {}", bins, r)
                }
                hawk_engine::core::VariableType::Categorical {
                    categories,
                    allow_unknown,
                } => {
                    format!(
                        "categorical &middot; {} categories{}",
                        categories.len(),
                        if *allow_unknown { " &middot; +unknown" } else { "" }
                    )
                }
            };
            let click_query = format!("SHOW {} AT {}:{}", v.name, first_dim_name, first_dim_value);
            format!(
                "<div class=\"schema-item schema-clickable\" onclick=\"fillAndSubmit('{q}')\">\
                 <span class=\"tag var\">var</span> <strong>{n}</strong> \
                 <span class=\"dim\">{d}</span></div>",
                q = click_query,
                n = v.name,
                d = detail,
            )
        })
        .collect();

    let dimensions_html: String = schema
        .dimensions
        .iter()
        .map(|d| {
            let gran = d.granularity.as_deref().unwrap_or("none");
            let click_query = format!("DIMENSIONS {}", d.name);
            format!(
                "<div class=\"schema-item schema-clickable\" onclick=\"fillAndSubmit('{q}')\">\
                 <span class=\"tag dim-tag\">dim</span> <strong>{n}</strong> \
                 <span class=\"dim\">source={src} &middot; granularity={g}</span></div>",
                q = click_query,
                n = d.name,
                src = d.source_column,
                g = gran,
            )
        })
        .collect();

    let joints_html: String = schema
        .joints
        .iter()
        .map(|(a, b)| {
            format!(
                "<div class=\"schema-item\"><span class=\"tag joint\">joint</span> <strong>{} &times; {}</strong></div>",
                a, b
            )
        })
        .collect();

    let var_name = schema.first_variable_name().unwrap_or("category");
    let dim_name = schema.dimensions.first().map(|d| d.name.as_str()).unwrap_or("time");
    let rank_q = format!("RANK {} BY ENTROPY OVER {}", var_name, dim_name);
    let show_q = format!("SHOW {} AT {}:2022", var_name, dim_name);

    let example_queries = vec![
        "STATS",
        "SCHEMA",
        "DIMENSIONS time",
        &rank_q,
        &show_q,
    ];

    let quick_buttons: String = example_queries
        .iter()
        .map(|q| {
            format!(
                "<button class=\"quick-btn\" onclick=\"document.getElementById('query-input').value='{}'; document.getElementById('query-input').form.requestSubmit()\">{}</button>",
                q, q
            )
        })
        .collect();

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hawk — {db_path}</title>
<script src="https://unpkg.com/htmx.org@2.0.4"></script>
<style>
{CSS}
</style>
</head>
<body>

<header>
  <div class="logo">Hawk</div>
  <div class="db-path">{db_path}</div>
</header>

<div class="stats-bar">
  <div class="stat"><span class="stat-val">{distributions}</span><span class="stat-label">distributions</span></div>
  <div class="stat"><span class="stat-val">{samples}</span><span class="stat-label">samples</span></div>
  <div class="stat"><span class="stat-val">{variables}</span><span class="stat-label">variables</span></div>
  <div class="stat"><span class="stat-val">{dimensions}</span><span class="stat-label">dimensions</span></div>
</div>

<div class="main">
  <div class="sidebar">
    <h3>Schema</h3>
    {variables_html}
    {dimensions_html}
    {joints_html}
    <h3 style="margin-top:1.5rem">Quick Queries</h3>
    <div class="quick-queries">
      {quick_buttons}
    </div>
    <h3 style="margin-top:1.5rem">History</h3>
    <div id="history-list" class="quick-queries"></div>
  </div>

  <div class="content">
    <form hx-get="/query" hx-target="#results" hx-indicator="#spinner" class="query-form" id="query-form">
      <input type="text" name="q" id="query-input" placeholder="COMPARE category BETWEEN time:2013 AND time:2022" autocomplete="off" autofocus>
      <button type="submit">Run</button>
      <span id="spinner" class="htmx-indicator">running...</span>
    </form>

    <div id="results" hx-get="/overview" hx-trigger="load" hx-swap="innerHTML">
    </div>
  </div>
</div>

<script>
(function() {{
  var MAX_HISTORY = 50;
  var STORAGE_KEY = 'hawk_query_history';

  function getHistory() {{
    try {{
      return JSON.parse(localStorage.getItem(STORAGE_KEY)) || [];
    }} catch(e) {{
      return [];
    }}
  }}

  function saveHistory(arr) {{
    localStorage.setItem(STORAGE_KEY, JSON.stringify(arr));
  }}

  function addToHistory(q) {{
    var hist = getHistory();
    hist = hist.filter(function(h) {{ return h !== q; }});
    hist.unshift(q);
    if (hist.length > MAX_HISTORY) hist = hist.slice(0, MAX_HISTORY);
    saveHistory(hist);
    renderHistory();
  }}

  function renderHistory() {{
    var container = document.getElementById('history-list');
    if (!container) return;
    var hist = getHistory();
    container.innerHTML = hist.map(function(q) {{
      var escaped = q.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
      return '<button class="quick-btn history-btn" onclick="fillAndSubmit(\'' + escaped + '\')">' + escaped + '</button>';
    }}).join('');
  }}

  window.fillAndSubmit = function(q) {{
    var input = document.getElementById('query-input');
    input.value = q;
    input.form.requestSubmit();
  }};

  var form = document.getElementById('query-form');
  form.addEventListener('submit', function() {{
    var q = document.getElementById('query-input').value.trim();
    if (q) addToHistory(q);
  }});
  form.addEventListener('htmx:beforeRequest', function() {{
    var q = document.getElementById('query-input').value.trim();
    if (q) addToHistory(q);
  }});

  renderHistory();
}})();
</script>

</body>
</html>"##,
        db_path = db_path,
        CSS = CSS,
        distributions = stats.distributions,
        samples = format_number(stats.total_samples),
        variables = stats.variables,
        dimensions = stats.dimensions,
        variables_html = variables_html,
        dimensions_html = dimensions_html,
        joints_html = joints_html,
        quick_buttons = quick_buttons,
    )
}

pub fn query_result(query: &str, result: &QueryResult, chart_html: &str) -> String {
    let mut html = String::new();

    html.push_str(&format!(
        "<div class=\"result-block\"><div class=\"query-echo\">{}</div>",
        html_escape(query)
    ));

    if !chart_html.is_empty() {
        html.push_str(&format!("<div class=\"chart\">{}</div>", chart_html));
    }

    html.push_str("<table><thead><tr>");
    for h in &result.header {
        html.push_str(&format!("<th>{}</th>", html_escape(h)));
    }
    html.push_str("</tr></thead><tbody>");

    for row in &result.rows {
        let is_separator = row.iter().all(|c| c.is_empty());
        if is_separator {
            html.push_str("<tr class=\"sep\"><td colspan=\"99\"></td></tr>");
            continue;
        }
        html.push_str("<tr>");
        for (i, cell) in row.iter().enumerate() {
            let class = if i == 0 { " class=\"label\"" } else { "" };
            // Color-code numeric values
            let content = colorize_cell(cell);
            html.push_str(&format!("<td{}>{}</td>", class, content));
        }
        html.push_str("</tr>");
    }

    html.push_str("</tbody></table></div>");
    html
}

pub fn query_error(query: &str, error: &str) -> String {
    format!(
        "<div class=\"result-block\"><div class=\"query-echo\">{}</div><div class=\"error\">{}</div></div>",
        html_escape(query),
        html_escape(error)
    )
}

fn colorize_cell(cell: &str) -> String {
    // Detect delta values like "+0.1234" or "-0.0567"
    if cell.starts_with('+') && cell.len() > 1 && cell[1..].starts_with(|c: char| c.is_ascii_digit()) {
        return format!("<span class=\"pos\">{}</span>", html_escape(cell));
    }
    if cell.starts_with('-') && cell.len() > 1 && cell[1..].starts_with(|c: char| c.is_ascii_digit()) {
        // Check if it's likely a delta (not just a negative number in other context)
        if cell.contains('.') {
            return format!("<span class=\"neg\">{}</span>", html_escape(cell));
        }
    }
    // Detect bar charts
    if cell.contains('#') {
        let parts: Vec<&str> = cell.splitn(2, '#').collect();
        if parts.len() == 2 {
            return format!(
                "{}<span class=\"bar\">{}</span>",
                html_escape(parts[0]),
                "#".repeat(parts[1].len() + 1)
            );
        }
    }
    // Detect shift markers
    if cell.contains("← shift") {
        return cell.replace("← shift", "<span class=\"shift\">← shift</span>");
    }
    html_escape(cell)
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

const CSS: &str = r##"
* { margin: 0; padding: 0; box-sizing: border-box; }

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
  background: #0d1117;
  color: #c9d1d9;
  min-height: 100vh;
}

header {
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 0.75rem 1.5rem;
  background: #161b22;
  border-bottom: 1px solid #30363d;
}

.logo {
  font-size: 1.25rem;
  font-weight: 700;
  color: #58a6ff;
  letter-spacing: -0.5px;
}

.db-path {
  color: #8b949e;
  font-size: 0.85rem;
  font-family: monospace;
}

.stats-bar {
  display: flex;
  gap: 2rem;
  padding: 0.75rem 1.5rem;
  background: #161b22;
  border-bottom: 1px solid #30363d;
}

.stat {
  display: flex;
  flex-direction: column;
}

.stat-val {
  font-size: 1.5rem;
  font-weight: 600;
  color: #f0f6fc;
}

.stat-label {
  font-size: 0.75rem;
  color: #8b949e;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.main {
  display: flex;
  height: calc(100vh - 110px);
}

.sidebar {
  width: 280px;
  min-width: 280px;
  padding: 1rem;
  background: #161b22;
  border-right: 1px solid #30363d;
  overflow-y: auto;
}

.sidebar h3 {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: #8b949e;
  margin-bottom: 0.5rem;
}

.schema-item {
  padding: 0.35rem 0;
  font-size: 0.85rem;
}

.schema-clickable {
  cursor: pointer;
  border-radius: 4px;
  padding: 0.35rem 0.25rem;
  transition: background 0.15s;
}

.schema-clickable:hover {
  background: #21262d;
}

.history-btn {
  color: #8b949e;
}

.tag {
  display: inline-block;
  padding: 0.1rem 0.4rem;
  border-radius: 3px;
  font-size: 0.7rem;
  font-weight: 600;
  text-transform: uppercase;
}

.tag.var { background: #1f3a5f; color: #58a6ff; }
.tag.dim-tag { background: #2a1f3f; color: #bc8cff; }
.tag.joint { background: #1f3f2a; color: #7ee787; }

.dim { color: #8b949e; font-size: 0.8rem; }

.quick-queries {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.quick-btn {
  background: #21262d;
  border: 1px solid #30363d;
  color: #c9d1d9;
  padding: 0.35rem 0.5rem;
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.75rem;
  font-family: monospace;
  text-align: left;
  transition: background 0.15s;
}

.quick-btn:hover { background: #30363d; }

.content {
  flex: 1;
  padding: 1rem 1.5rem;
  overflow-y: auto;
}

.query-form {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 1rem;
}

.query-form input {
  flex: 1;
  padding: 0.6rem 0.75rem;
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 6px;
  color: #c9d1d9;
  font-family: 'SF Mono', 'Fira Code', monospace;
  font-size: 0.9rem;
  outline: none;
  transition: border-color 0.15s;
}

.query-form input:focus {
  border-color: #58a6ff;
}

.query-form button {
  padding: 0.6rem 1.25rem;
  background: #238636;
  border: 1px solid #2ea043;
  border-radius: 6px;
  color: #fff;
  font-weight: 600;
  cursor: pointer;
  transition: background 0.15s;
}

.query-form button:hover { background: #2ea043; }

.htmx-indicator {
  color: #8b949e;
  font-size: 0.85rem;
  align-self: center;
  display: none;
}

.htmx-request .htmx-indicator { display: inline; }

.result-block {
  margin-bottom: 1.5rem;
}

.query-echo {
  font-family: 'SF Mono', 'Fira Code', monospace;
  font-size: 0.8rem;
  color: #8b949e;
  padding: 0.4rem 0;
  border-bottom: 1px solid #21262d;
  margin-bottom: 0.5rem;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.85rem;
  font-family: 'SF Mono', 'Fira Code', monospace;
}

thead th {
  text-align: left;
  padding: 0.5rem 0.75rem;
  border-bottom: 2px solid #30363d;
  color: #8b949e;
  font-weight: 600;
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

tbody td {
  padding: 0.35rem 0.75rem;
  border-bottom: 1px solid #21262d;
}

tbody tr:hover { background: #161b22; }
tr.sep td { padding: 0.15rem; border: none; }

td.label { color: #f0f6fc; font-weight: 500; }

.pos { color: #7ee787; }
.neg { color: #f85149; }
.shift { color: #d29922; font-weight: 600; }
.bar { color: #58a6ff; }

.error {
  background: #3d1c1c;
  border: 1px solid #f85149;
  color: #f85149;
  padding: 0.75rem 1rem;
  border-radius: 6px;
  font-family: monospace;
  font-size: 0.85rem;
}

.chart {
  margin: 0.75rem 0;
}

.chart svg {
  width: 100%;
  max-width: 700px;
}
"##;

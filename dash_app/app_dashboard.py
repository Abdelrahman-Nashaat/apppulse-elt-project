import duckdb
import pandas as pd
from dash import Dash, dcc, html
import plotly.express as px

# --- Load data from warehouse (DuckDB)
WAREHOUSE_PATH = "/workspaces/apppulse-elt-project/warehouse/warehouse.duckdb"
conn = duckdb.connect("/workspaces/apppulse-elt-project/warehouse/apppulse.duckdb", read_only=True)

# Load fact table
df = conn.execute("SELECT * FROM main.fact_app_metrics").fetchdf()

# --- Initialize Dash app
app = Dash(__name__)
app.title = "AppPulse Analytics Dashboard"

# --- Charts
fig_sentiment = px.histogram(
    df, x="avg_sentiment", nbins=20,
    title="Distribution of Average Sentiment per App",
    color_discrete_sequence=["#00CC96"]
)

fig_reviews = px.bar(
    df.sort_values("total_reviews", ascending=False).head(10),
    x="app_name", y="total_reviews",
    title="Top 10 Apps by Review Count",
    color="total_reviews", color_continuous_scale="Viridis"
)

fig_category = px.bar(
    df.groupby("app_category", as_index=False)["total_reviews"].sum().sort_values("total_reviews", ascending=False).head(10),
    x="app_category", y="total_reviews",
    title="Top 10 Categories by Reviews",
    color="total_reviews", color_continuous_scale="Plasma"
)

# --- Layout
app.layout = html.Div([
    html.H1("📊 AppPulse Analytics Dashboard", style={'textAlign': 'center'}),
    html.P("Explore app performance, sentiment, and user engagement trends.", style={'textAlign': 'center'}),
    html.Br(),
    dcc.Graph(figure=fig_sentiment),
    dcc.Graph(figure=fig_reviews),
    dcc.Graph(figure=fig_category),
])

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)

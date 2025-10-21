import dash
import dash_bootstrap_components as dbc # Using Bootstrap for a better layout
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import duckdb
import os

# --- 1. Database Connection Setup ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "warehouse", "apppulse.duckdb")

def load_data():
    """
    Connects to DuckDB and fetches data in two separate, optimized queries
    to prevent data duplication (fan-out).
    """
    print(f"Connecting to DuckDB at: {DB_PATH}")
    try:
        conn = duckdb.connect(database=DB_PATH, read_only=True)
        
        # Query 1: For aggregated metrics (KPIs, charts about apps)
        metrics_query = """
        SELECT
            da.app_name,
            da.developer_name,
            dc.category_name,
            fm.install_size_mb,
            fm.price,
            fm.average_user_rating,
            fm.total_installs,
            fm.total_reviews,
            fm.last_updated_date
        FROM main.fact_app_metrics fm
        JOIN main.dim_apps da ON fm.app_id = da.app_id
        JOIN main.dim_categories dc ON fm.category_id = dc.category_id
        """
        df_metrics = conn.execute(metrics_query).fetchdf()
        print(f"Metrics data loaded successfully. Rows: {len(df_metrics)}")

        # Query 2: For review-level data (Sentiment analysis)
        reviews_query = """
        SELECT 
            da.app_name,
            dc.category_name,
            sr.review_sentiment
        FROM main.stg_reviews sr
        JOIN main.dim_apps da ON sr.app_id = da.app_id
        JOIN main.fact_app_metrics fm ON da.app_id = fm.app_id
        JOIN main.dim_categories dc ON fm.category_id = dc.category_id
        WHERE sr.review_sentiment IS NOT NULL
        """
        df_reviews = conn.execute(reviews_query).fetchdf()
        print(f"Reviews data loaded successfully. Rows: {len(df_reviews)}")
        
        conn.close()
        return df_metrics, df_reviews

    except Exception as e:
        print(f"An error occurred during data loading: {e}")
        # Return empty DataFrames to prevent the app from crashing
        return pd.DataFrame(), pd.DataFrame()

# Load data once at the start
df_metrics, df_reviews = load_data()

# --- 2. Initialize Dash App with Bootstrap Theme ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# --- 3. App Layout ---
available_categories = sorted(df_metrics['category_name'].unique()) if not df_metrics.empty else []

app.layout = dbc.Container(fluid=True, style={'backgroundColor': '#f8f9fa'}, children=[
    
    # Header
    dbc.Row(
        dbc.Col(html.H1("AppPulse Analytics Dashboard", className="text-center text-primary my-4"), width=12)
    ),
    
    # Filters
    dbc.Row(
        dbc.Col(
            dcc.Dropdown(
                id='category-dropdown',
                options=[{'label': cat, 'value': cat} for cat in available_categories],
                value=None,
                placeholder="Select a Category (or view All)",
            ),
            width=12,
            className="mb-4"
        )
    ),
    
    # KPIs
    dbc.Row(id='kpi-cards', className="mb-4"),
    
    # Charts
    dbc.Row([
        dbc.Col(dcc.Graph(id='top-rated-apps'), width=6),
        dbc.Col(dcc.Graph(id='rating-installs-scatter'), width=6),
    ]),
    
    dbc.Row(
        dbc.Col(dcc.Graph(id='sentiment-summary'), width=12, className="mt-4")
    )
])

# --- 4. Helper function for creating KPI cards ---
def create_kpi_card(title, value, color):
    return dbc.Col(
        dbc.Card(
            dbc.CardBody([
                html.H4(title, className="card-title text-center"),
                html.P(f"{value}", className=f"card-text text-center text-{color} h3")
            ])
        )
    )

# --- 5. Callbacks for Interactivity ---
@app.callback(
    [Output('kpi-cards', 'children'),
     Output('top-rated-apps', 'figure'),
     Output('rating-installs-scatter', 'figure'),
     Output('sentiment-summary', 'figure')],
    [Input('category-dropdown', 'value')]
)
def update_dashboard(selected_category):
    # Handle case where data loading failed
    if df_metrics.empty or df_reviews.empty:
        error_card = dbc.Col(dbc.Alert("Error loading data from DuckDB. Please run the pipeline and refresh.", color="danger"))
        return [error_card], {}, {}, {}

    # Filter data based on selection
    if selected_category:
        filtered_metrics_df = df_metrics[df_metrics['category_name'] == selected_category]
        
        # Filter reviews based on apps available in the selected category
        apps_in_category = filtered_metrics_df['app_name'].unique()
        filtered_reviews_df = df_reviews[df_reviews['app_name'].isin(apps_in_category)]
        
        title_suffix = f" in {selected_category}"
    else:
        filtered_metrics_df = df_metrics
        filtered_reviews_df = df_reviews
        title_suffix = " (All Categories)"

    # --- 5.1. KPIs ---
    total_apps = filtered_metrics_df['app_name'].nunique()
    avg_rating = filtered_metrics_df['average_user_rating'].mean().round(2)
    total_installs = filtered_metrics_df['total_installs'].sum()
    
    kpi_cards = [
        create_kpi_card("Total Unique Apps", f"{total_apps:,}", "primary"),
        create_kpi_card("Average Rating", f"{avg_rating} ★", "warning"),
        create_kpi_card("Total Installs", f"{total_installs:,}", "success")
    ]
    
    # --- 5.2. Top 10 Rated Apps (Bar Chart) ---
    top_apps = filtered_metrics_df.nlargest(10, 'average_user_rating')
    fig_rated = px.bar(
        top_apps,
        x='average_user_rating',
        y='app_name',
        orientation='h',
        title=f"Top 10 Rated Apps{title_suffix}",
        labels={'average_user_rating': 'Average Rating', 'app_name': 'App Name'},
        template='bootstrap'
    )
    fig_rated.update_layout(yaxis={'categoryorder':'total ascending'})

    # --- 5.3. Rating vs. Installs (Scatter Plot) ---
    fig_scatter = px.scatter(
        filtered_metrics_df,
        x='average_user_rating',
        y='total_installs',
        size='total_installs',
        color='price',
        hover_name='app_name',
        log_y=True,
        title=f"Rating vs. Installs{title_suffix}",
        labels={'average_user_rating': 'Average Rating', 'total_installs': 'Total Installs (Log Scale)'},
        template='bootstrap'
    )

    # --- 5.4. Sentiment Summary (Pie Chart) ---
    # Use the separate, correct reviews dataframe
    sentiment_counts = filtered_reviews_df['review_sentiment'].value_counts().reset_index()
    sentiment_counts.columns = ['Sentiment', 'Count']
    
    color_map = {'Positive': 'green', 'Negative': 'red', 'Neutral': 'grey'}
    
    fig_sentiment = px.pie(
        sentiment_counts,
        values='Count',
        names='Sentiment',
        title=f"User Review Sentiment{title_suffix}",
        color='Sentiment',
        color_discrete_map=color_map,
        template='bootstrap'
    )
    fig_sentiment.update_traces(textposition='inside', textinfo='percent+label')

    return kpi_cards, fig_rated, fig_scatter, fig_sentiment


# --- 6. Run the App ---
if __name__ == '__main__':
    print("Starting Dash server...")
    app.run_server(debug=True)
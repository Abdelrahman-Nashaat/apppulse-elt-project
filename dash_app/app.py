import dash
from dash import dcc, html # Use this import style
import plotly.express as px
import plotly.graph_objects as go # <<< ADDED IMPORT
import pandas as pd
import duckdb
import os
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc # Keep bootstrap for basic styling
import numpy as np # <<< ADDED IMPORT

# 1. تحديد مسار قاعدة بيانات DuckDB
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "warehouse", "apppulse.duckdb")

# 2. تحميل البيانات الأولية (بالأسماء الصحيحة المؤكدة)
def load_data_from_duckdb():
    """يتصل بـ DuckDB ويستخلص البيانات (بالأسماء الصحيحة المؤكدة)."""
    print(f"Connecting to DuckDB at: {DB_PATH}")
    df = pd.DataFrame() # Initialize empty
    try:
        conn = duckdb.connect(database=DB_PATH, read_only=True)

        # --- Debug: Check if tables exist ---
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Tables in DuckDB: {tables}")
        # Update required tables based on final dbt models
        required_tables = {'fact_app_metrics', 'dim_apps', 'dim_categories', 'stg_reviews'}
        available_tables = {t[0] for t in tables}

        if not required_tables.issubset(available_tables):
             missing_tables = required_tables - available_tables
             print(f"❌ Error: Not all required tables ({missing_tables}) found in DuckDB. Please ensure dbt run completed successfully.")
             conn.close()
             return df

        # --- الاستعلام الرئيسي المصحح ---
        # Using confirmed correct column names from dbt models:
        # dc.app_category, da.app_size_bytes, fm.app_price, da.last_updated_date
        # Joining reviews on app_name (assuming stg_reviews has app_name)
        # Assuming dim_apps contains the necessary descriptive fields
        query = """
        SELECT
            da.app_name,
            -- developer_name is confirmed NOT available in dim_apps
            dc.app_category AS category_name, -- Correct name from dim_categories
            da.app_size_bytes,       -- Correct name from dim_apps
            fm.app_price AS price,          -- Correct name from fact_app_metrics
            fm.average_user_rating,
            fm.total_installs,
            fm.total_reviews,
            da.last_updated_date,     -- Correct name from dim_apps
            sr.review_sentiment      -- From stg_reviews
        FROM main.fact_app_metrics fm
        JOIN main.dim_apps da ON fm.app_id = da.app_id
        JOIN main.dim_categories dc ON fm.category_id = dc.category_id
        -- Use LEFT JOIN for reviews in case some apps have no reviews in stg_reviews
        -- Join using app_name as confirmed for stg_reviews structure
        LEFT JOIN main.stg_reviews sr ON da.app_name = sr.app_name
        """

        df = conn.execute(query).fetchdf()

        # Add conversion for size AFTER fetching
        if 'app_size_bytes' in df.columns:
             # Use safe division: convert to numeric, fillna, then divide
             # Ensure the column is numeric first
             size_bytes_numeric = pd.to_numeric(df['app_size_bytes'], errors='coerce').fillna(0)
             df['app_size_mb'] = size_bytes_numeric / (1024*1024)
             # Handle infinities if size_bytes_numeric was 0 resulting in NaN/Inf after division
             df['app_size_mb'] = df['app_size_mb'].replace([np.inf, -np.inf], 0).fillna(0) # Also fill potential NaNs from division

        else:
             print("⚠️ Warning: 'app_size_bytes' column not found for size conversion.")
             df['app_size_mb'] = 0.0 # Add column anyway

        conn.close()
        print(f"✅ Data loaded successfully. Total rows: {len(df)}")


    except duckdb.IOException:
        print(f"❌ Error: Database file not found at {DB_PATH}. Run the Airflow DAG first!")
    except duckdb.BinderException as e: # Catch column name errors specifically
        print(f"❌ Binder Error: Problem with column names in SQL query: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during data loading: {e}")

    # Ensure all required columns exist even if data loading failed partially
    required_cols_final = ['app_name', 'category_name', 'average_user_rating', 'total_installs', 'total_reviews', 'price', 'review_sentiment', 'app_size_mb']
    for col in required_cols_final:
        if col not in df.columns:
            print(f"⚠️ Warning: Essential column '{col}' missing after potential error. Adding default.")
            if 'rating' in col or 'price' in col or 'size' in col: df[col] = 0.0
            elif 'installs' in col or 'reviews' in col: df[col] = 0
            elif col == 'category_name': df[col] = 'Unknown'
            else: df[col] = None

    return df


# تحميل البيانات في متغير عام
app_data_df = load_data_from_duckdb()

# تهيئة تطبيق Dash (Using simple LUX theme)
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])
app.title = "AppPulse Analytics"

# --- 3. تصميم لوحة التحكم (Layout - Simplified back to original structure) ---

available_categories = sorted(app_data_df['category_name'].dropna().unique()) if 'category_name' in app_data_df.columns else []

app.layout = dbc.Container(fluid=True, style={'backgroundColor': '#f8f9fa', 'padding': '20px'}, children=[

    html.H1(
        children='AppPulse Analytics Dashboard',
        style={'textAlign': 'center', 'color': '#007bff', 'marginBottom': '30px'}
    ),

    # Category Filter
    dbc.Row([
        dbc.Col(width=3), # Spacer
        dbc.Col(
            dcc.Dropdown(
                id='category-dropdown',
                options=[{'label': cat, 'value': cat} for cat in available_categories],
                value=None,
                placeholder="Select a category or view All",
            ),
            width=6 # Centered dropdown
        ),
         dbc.Col(width=3) # Spacer
    ], className="mb-4"),


    # KPIs
    dbc.Row(id='kpi-output', className="mb-4 justify-content-center"), # Center KPIs

    html.Hr(),

    # Charts
    dbc.Row([
        dbc.Col(dcc.Graph(id='top-rated-apps'), width=12, md=6, className="mb-3"),
        dbc.Col(dcc.Graph(id='rating-installs-scatter'), width=12, md=6, className="mb-3"),
    ]),
     dbc.Row([
        dbc.Col(dcc.Graph(id='sentiment-summary'), width=12, md=6, className="mb-3"),
        # Placeholder removed, let sentiment take full width on small screens or adjust layout
        dbc.Col(html.Div(id='placeholder-for_future_chart'), width=12, md=6, className="mb-3")
    ])
])

# --- 4. وظائف الاتصال التفاعلية (Callbacks - Adapted from Original) ---

# Original simple KPI card renderer
def _render_kpi_card(title, value):
    """ Renders a simple KPI card. """
    return dbc.Col(dbc.Card([
        dbc.CardHeader(title),
        dbc.CardBody(html.H4(f"{value}", className="card-title text-center"))
    ]), width=12, sm=6, md=4, className="text-center mb-3") # Adjusted grid for 3 cards

@app.callback(
    [Output('kpi-output', 'children'),
     Output('top-rated-apps', 'figure'),
     Output('rating-installs-scatter', 'figure'),
     Output('sentiment-summary', 'figure'),
     Output('placeholder-for_future_chart', 'children')], # Output for the placeholder div
    [Input('category-dropdown', 'value')]
)
def update_graph(selected_category):
    """Updates KPIs and charts based on selected category."""
    # Use the global app_data_df which was loaded at startup
    global app_data_df

    if app_data_df.empty:
        error_msg = dbc.Alert("⚠️ Error loading data from DuckDB or DB is empty. Please ensure the Airflow DAG ran successfully and created data.", color="danger")
        empty_fig = go.Figure().update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        # Ensure the output structure matches the number of outputs
        return [dbc.Row(dbc.Col(error_msg, width=12))], empty_fig, empty_fig, empty_fig, ""

    # Filter data
    if selected_category and 'category_name' in app_data_df.columns:
        # Ensure filtering doesn't fail if category_name column ended up with None/NaN
        filtered_df = app_data_df.loc[app_data_df['category_name'].fillna('Unknown') == selected_category].copy()
        title_suffix = f" in {selected_category}"
    else:
        filtered_df = app_data_df.copy()
        title_suffix = " (All Categories)"

    print(f"Callback triggered. Category: {selected_category}. Filtered rows: {len(filtered_df)}")

    # Check if filtered data is empty after filtering
    if filtered_df.empty:
         no_data_msg = [dbc.Row(dbc.Col(dbc.Alert(f"No data available for {selected_category or 'any category'}.", color="info"), width=12))]
         empty_fig = go.Figure().update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
         # Ensure the output structure matches the number of outputs
         return no_data_msg, empty_fig, empty_fig, empty_fig, ""


    # --- KPIs ---
    kpi_cards_content = []
    try:
        total_apps = filtered_df['app_name'].nunique()
        avg_rating = filtered_df['average_user_rating'].mean()
        avg_rating = round(avg_rating, 2) if pd.notna(avg_rating) else 0.0
        # Ensure total_installs column exists before summing
        total_installs = int(filtered_df['total_installs'].sum()) if 'total_installs' in filtered_df.columns else 0

        # Format large numbers
        if total_installs >= 1_000_000_000: installs_display = f"{total_installs / 1_000_000_000:.1f}B"
        elif total_installs >= 1_000_000: installs_display = f"{total_installs / 1_000_000:.1f}M"
        elif total_installs >= 1_000: installs_display = f"{total_installs / 1_000:.1f}K"
        else: installs_display = f"{total_installs:,}"

        # Ensure KPIs are returned as a list of dbc.Col elements for the dbc.Row
        kpi_cards_content = [
            _render_kpi_card("Total Apps", f"{total_apps:,}"),
            _render_kpi_card("Avg Rating", f"{avg_rating} ⭐"),
            _render_kpi_card("Total Installs", installs_display)
        ]
    except Exception as e:
        print(f"Error calculating KPIs: {e}")
        # Return error message within a Col structure
        kpi_cards_content = [dbc.Col(dbc.Alert("Error calculating KPIs.", color="warning", className="text-center"), width=12)]

    chart_height = 400
    chart_layout_defaults = dict(height=chart_height, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=40, r=20, t=60, b=40))

    # --- Top Rated Apps ---
    fig_rated = go.Figure().update_layout(title="Top Rated Apps", **chart_layout_defaults)
    try:
        if 'app_name' in filtered_df.columns and 'average_user_rating' in filtered_df.columns:
            # Aggregate first to handle potential duplicate app names
            top_apps_data = filtered_df.groupby('app_name')['average_user_rating'].mean().reset_index()
            top_n = min(10, len(top_apps_data))
            if top_n > 0:
                 top_apps = top_apps_data.nlargest(top_n, 'average_user_rating')
                 fig_rated = px.bar(top_apps, x='average_user_rating', y='app_name', orientation='h',
                                   title=f"🏆 Top {top_n} Rated Apps{title_suffix}",
                                   labels={'average_user_rating': 'Avg. Rating', 'app_name': ''})
                 fig_rated.update_layout(yaxis={'categoryorder':'total ascending'}, **chart_layout_defaults)
                 fig_rated.update_traces(texttemplate='%{x:.2f}', textposition='outside')
            else:
                 fig_rated.update_layout(title=f"🏆 Top Rated Apps{title_suffix} (No data)")
        else: fig_rated.update_layout(title="🏆 Top Rated Apps (Missing Data)")
    except Exception as e:
        print(f"Error creating top rated apps chart: {e}")
        fig_rated.update_layout(title="Error loading Top Rated Apps")

    # --- Rating vs Installs ---
    fig_scatter = go.Figure().update_layout(title="Rating vs Installs", **chart_layout_defaults)
    try:
        scatter_cols = ['app_name', 'average_user_rating', 'total_installs', 'price']
        if all(col in filtered_df.columns for col in scatter_cols):
            # Aggregate first
            df_scatter_agg = filtered_df.groupby('app_name').agg({
                'average_user_rating': 'mean', 'total_installs': 'sum', 'price': 'first'
            }).reset_index()
            sample_size = min(2000, len(df_scatter_agg))
            sample_df = df_scatter_agg.sample(n=sample_size) if len(df_scatter_agg) > sample_size else df_scatter_agg
            if not sample_df.empty:
                # Ensure 'price' column is numeric for coloring, handle non-numeric gracefully
                sample_df['price_numeric_viz'] = pd.to_numeric(sample_df['price'], errors='coerce').fillna(0)

                fig_scatter = px.scatter(sample_df, x='average_user_rating', y='total_installs',
                                         size='total_installs', color='price_numeric_viz', hover_name='app_name',
                                         log_y=True, title=f"📈 Rating vs. Installs{title_suffix}",
                                         labels={'average_user_rating': 'Avg. Rating', 'total_installs': 'Installs (Log Scale)', 'price_numeric_viz': 'Price'},
                                         size_max=50, color_continuous_scale=px.colors.sequential.Viridis) # Changed color scale
                fig_scatter.update_layout(**chart_layout_defaults)
            else: fig_scatter.update_layout(title=f"📈 Rating vs. Installs{title_suffix} (No data)")
        else: fig_scatter.update_layout(title="📈 Rating vs Installs (Missing Data)")
    except Exception as e:
        print(f"Error creating scatter plot: {e}")
        fig_scatter.update_layout(title="Error loading Rating vs Installs")

    # --- Sentiment Summary ---
    fig_sentiment = go.Figure().update_layout(title="Sentiment Summary", **chart_layout_defaults)
    try:
         # Use the original 'review_sentiment' column which should exist now
        if 'review_sentiment' in filtered_df.columns:
            # Drop NaN before value_counts
            sentiment_counts_series = filtered_df['review_sentiment'].dropna().value_counts()
            if not sentiment_counts_series.empty:
                sentiment_counts = sentiment_counts_series.reset_index()
                sentiment_counts.columns = ['Sentiment', 'Count']
                colors = {'Positive': '#28a745', 'Negative': '#dc3545', 'Neutral': '#6c757d'}
                sentiment_colors = [colors.get(s, '#adb5bd') for s in sentiment_counts['Sentiment']]
                fig_sentiment = go.Figure(data=[go.Pie(
                    labels=sentiment_counts['Sentiment'], values=sentiment_counts['Count'],
                    marker=dict(colors=sentiment_colors, line=dict(color='#ffffff', width=1)),
                    textinfo='percent+label', hoverinfo='label+percent+value', insidetextorientation='radial',
                    sort=False # Keep original order if needed
                )])
                fig_sentiment.update_layout(title=dict(text=f"💬 User Sentiment Distribution{title_suffix}"),
                                            showlegend=False,
                                            margin=dict(l=20, r=20, t=60, b=40),
                                            **chart_layout_defaults)
            else: fig_sentiment.update_layout(title=f"💬 User Sentiment{title_suffix} (No data)")
        else: fig_sentiment.update_layout(title="💬 User Sentiment (Missing Data)")
    except Exception as e:
        print(f"Error creating sentiment chart: {e}")
        fig_sentiment.update_layout(title="Error loading User Sentiment")

    placeholder_content = "" # Keep placeholder empty

    # Ensure kpi_cards_content is a list
    if not isinstance(kpi_cards_content, list):
        kpi_cards_content = [kpi_cards_content]

    return kpi_cards_content, fig_rated, fig_scatter, fig_sentiment, placeholder_content

# --- 6. Run Server ---
if __name__ == '__main__':
    print("Starting Dash server...")
    app.run_server(debug=True, host='0.0.0.0', port=8050) # Use 0.0.0.0 for Codespaces
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import duckdb
import os

# --- 1. Database Connection Setup ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "warehouse", "apppulse.duckdb")

def load_data():
    """
    Connects to DuckDB and fetches metrics and reviews data.
    """
    print(f"Connecting to DuckDB at: {DB_PATH}")
    try:
        conn = duckdb.connect(database=DB_PATH, read_only=True)
        
        # Debug: Check table counts
        print("=== Table Row Counts ===")
        try:
            print(f"fact_app_metrics: {conn.execute('SELECT COUNT(*) FROM main.fact_app_metrics').fetchone()[0]}")
            print(f"dim_apps: {conn.execute('SELECT COUNT(*) FROM main.dim_apps').fetchone()[0]}")
            print(f"dim_categories: {conn.execute('SELECT COUNT(*) FROM main.dim_categories').fetchone()[0]}")
            print(f"stg_reviews: {conn.execute('SELECT COUNT(*) FROM main.stg_reviews').fetchone()[0]}")
        except Exception as debug_err:
            print(f"Debug count error: {debug_err}")
        print("========================")
        
        # Query 1: Simplified - get data directly without complex joins
        metrics_query = """
        SELECT
            da.app_name,
            da.app_category AS category_name,
            da.app_size_bytes AS install_size_bytes,
            da.app_price AS price,
            da.last_updated_date,
            fm.average_user_rating,
            fm.total_installs,
            fm.total_reviews
        FROM main.fact_app_metrics fm
        JOIN main.dim_apps da ON fm.app_id = da.app_id
        """
        df_metrics = conn.execute(metrics_query).fetchdf()
        print(f"Metrics data loaded successfully. Rows: {len(df_metrics)}")

        # Query 2: Simplified reviews query
        reviews_query = """
        SELECT 
            da.app_name,
            da.app_category AS category_name,
            sr.review_sentiment
        FROM main.stg_reviews sr
        JOIN main.dim_apps da ON sr.app_name = da.app_name
        WHERE sr.review_sentiment IS NOT NULL
        """
        df_reviews = conn.execute(reviews_query).fetchdf()
        print(f"Reviews data loaded successfully. Rows: {len(df_reviews)}")
        
        conn.close()
        return df_metrics, df_reviews

    except Exception as e:
        print(f"An error occurred during data loading: {e}")
        return pd.DataFrame(), pd.DataFrame()

# Load data once at startup
df_metrics, df_reviews = load_data()

# --- 2. Initialize Dash App with Modern Theme ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX])

# --- 3. Enhanced Layout ---
available_categories = sorted(df_metrics['category_name'].unique()) if not df_metrics.empty else []

app.layout = dbc.Container(fluid=True, style={
    'backgroundColor': '#f5f7fa',
    'minHeight': '100vh',
    'padding': '20px'
}, children=[
    # Modern Header with gradient
    dbc.Row(
        dbc.Col([
            html.Div([
                html.H1("📱 AppPulse Analytics", 
                       style={
                           'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                           'WebkitBackgroundClip': 'text',
                           'WebkitTextFillColor': 'transparent',
                           'fontWeight': 'bold',
                           'fontSize': '3rem',
                           'marginBottom': '5px'
                       }),
                html.P("Real-time insights from Google Play Store data",
                      style={
                          'color': '#6c757d',
                          'fontSize': '1.1rem',
                          'marginBottom': '0'
                      })
            ], style={'textAlign': 'center', 'padding': '20px 0'})
        ], width=12)
    ),
    
    # Filter Section with modern card
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Label("🔍 Filter by Category", 
                              style={'fontWeight': 'bold', 'marginBottom': '10px', 'color': '#495057'}),
                    dcc.Dropdown(
                        id='category-dropdown',
                        options=[{'label': f'📁 {cat}', 'value': cat} for cat in available_categories],
                        value=None,
                        placeholder="All Categories",
                        style={'borderRadius': '8px'}
                    )
                ])
            ], style={'boxShadow': '0 4px 6px rgba(0,0,0,0.07)', 'borderRadius': '12px', 'border': 'none'})
        ], width=12, className="mb-4")
    ]),
    
    # KPI Cards Row
    dbc.Row(id='kpi-cards', className="mb-4"),
    
    # Charts Row 1
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id='top-rated-apps', config={'displayModeBar': False})
                ])
            ], style={'boxShadow': '0 4px 6px rgba(0,0,0,0.07)', 'borderRadius': '12px', 'border': 'none', 'height': '100%'})
        ], width=12, lg=6, className="mb-4"),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id='rating-installs-scatter', config={'displayModeBar': False})
                ])
            ], style={'boxShadow': '0 4px 6px rgba(0,0,0,0.07)', 'borderRadius': '12px', 'border': 'none', 'height': '100%'})
        ], width=12, lg=6, className="mb-4"),
    ]),
    
    # Charts Row 2
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id='sentiment-summary', config={'displayModeBar': False})
                ])
            ], style={'boxShadow': '0 4px 6px rgba(0,0,0,0.07)', 'borderRadius': '12px', 'border': 'none'})
        ], width=12, lg=6, className="mb-4"),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dcc.Graph(id='category-distribution', config={'displayModeBar': False})
                ])
            ], style={'boxShadow': '0 4px 6px rgba(0,0,0,0.07)', 'borderRadius': '12px', 'border': 'none'})
        ], width=12, lg=6, className="mb-4"),
    ]),
    
    # Footer
    dbc.Row([
        dbc.Col([
            html.Div([
                html.P("Built with ❤️ using Dash, DuckDB, and dbt", 
                      style={'textAlign': 'center', 'color': '#6c757d', 'marginTop': '20px'})
            ])
        ], width=12)
    ])
])

# --- 4. Enhanced KPI Card Helper ---
def create_kpi_card(title, value, icon, color, gradient):
    return dbc.Col([
        dbc.Card([
            dbc.CardBody([
                html.Div([
                    html.Div([
                        html.Span(icon, style={'fontSize': '2.5rem'}),
                    ], style={'marginBottom': '10px'}),
                    html.H6(title, style={'color': '#6c757d', 'fontWeight': '500', 'marginBottom': '10px'}),
                    html.H3(str(value), style={
                        'color': color,
                        'fontWeight': 'bold',
                        'margin': '0'
                    })
                ], style={'textAlign': 'center'})
            ])
        ], style={
            'background': gradient,
            'border': 'none',
            'borderRadius': '12px',
            'boxShadow': '0 4px 6px rgba(0,0,0,0.07)',
            'height': '100%'
        })
    ], width=12, lg=4, className="mb-3")

# --- 5. Callbacks ---
@app.callback(
    [Output('kpi-cards', 'children'),
     Output('top-rated-apps', 'figure'),
     Output('rating-installs-scatter', 'figure'),
     Output('sentiment-summary', 'figure'),
     Output('category-distribution', 'figure')],
    [Input('category-dropdown', 'value')]
)
def update_dashboard(selected_category):
    # Handle empty data
    if df_metrics.empty or df_reviews.empty:
        error_card = dbc.Col(dbc.Alert(
            "⚠️ Error loading data from DuckDB. Please run the pipeline and refresh.", 
            color="danger", style={'borderRadius': '12px'}))
        empty_fig = go.Figure()
        return [error_card], empty_fig, empty_fig, empty_fig, empty_fig

    # Filter by category
    if selected_category:
        filtered_metrics_df = df_metrics[df_metrics['category_name'] == selected_category]
        apps_in_category = filtered_metrics_df['app_name'].unique()
        filtered_reviews_df = df_reviews[df_reviews['app_name'].isin(apps_in_category)]
        title_suffix = f" in {selected_category}"
    else:
        filtered_metrics_df = df_metrics
        filtered_reviews_df = df_reviews
        title_suffix = ""

    print(f"Filtered metrics: {len(filtered_metrics_df)} rows")
    print(f"Filtered reviews: {len(filtered_reviews_df)} rows")

    # --- KPIs ---
    total_apps = filtered_metrics_df['app_name'].nunique()
    avg_rating = filtered_metrics_df['average_user_rating'].mean()
    if pd.notna(avg_rating):
        avg_rating = round(avg_rating, 2)
    else:
        avg_rating = 0
    total_installs = int(filtered_metrics_df['total_installs'].sum())
    
    # Format large numbers
    if total_installs >= 1_000_000_000_000:
        installs_display = f"{total_installs / 1_000_000_000_000:.2f}T"
    elif total_installs >= 1_000_000_000:
        installs_display = f"{total_installs / 1_000_000_000:.2f}B"
    elif total_installs >= 1_000_000:
        installs_display = f"{total_installs / 1_000_000:.2f}M"
    else:
        installs_display = f"{total_installs:,}"
    
    kpi_cards = [
        create_kpi_card("Total Apps", f"{total_apps:,}", "📱", "#667eea", "linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)"),
        create_kpi_card("Average Rating", f"{avg_rating} ⭐", "📊", "#f093fb", "linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)"),
        create_kpi_card("Total Installs", installs_display, "⬇️", "#4facfe", "linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)")
    ]
    
    # --- Top Rated Apps ---
    try:
        top_apps = filtered_metrics_df.nlargest(10, 'average_user_rating')
        if len(top_apps) > 0:
            fig_rated = go.Figure(go.Bar(
                x=top_apps['average_user_rating'],
                y=top_apps['app_name'],
                orientation='h',
                marker=dict(
                    color=top_apps['average_user_rating'],
                    colorscale='Viridis',
                    showscale=False
                ),
                text=top_apps['average_user_rating'].round(2),
                textposition='auto',
            ))
            fig_rated.update_layout(
                title=dict(text=f"🏆 Top 10 Rated Apps{title_suffix}", font=dict(size=18, color='#2c3e50')),
                xaxis_title="Rating",
                yaxis_title="",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                yaxis={'categoryorder':'total ascending'},
                margin=dict(l=20, r=20, t=60, b=40),
                height=400
            )
        else:
            fig_rated = go.Figure()
    except Exception as e:
        print(f"Error creating top rated apps chart: {e}")
        fig_rated = go.Figure()

    # --- Rating vs Installs ---
    try:
        sample_size = min(1000, len(filtered_metrics_df))
        sample_df = filtered_metrics_df.sample(n=sample_size) if len(filtered_metrics_df) > sample_size else filtered_metrics_df
        
        fig_scatter = px.scatter(
            sample_df,
            x='average_user_rating',
            y='total_installs',
            size='total_installs',
            color='average_user_rating',
            hover_name='app_name',
            log_y=True,
            color_continuous_scale='Plasma',
            title=f"📈 Rating vs. Installs{title_suffix}",
            labels={'average_user_rating': 'Rating', 'total_installs': 'Installs (Log Scale)'}
        )
        fig_scatter.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            title=dict(font=dict(size=18, color='#2c3e50')),
            margin=dict(l=20, r=20, t=60, b=40),
            height=400
        )
    except Exception as e:
        print(f"Error creating scatter chart: {e}")
        fig_scatter = go.Figure()

    # --- Sentiment Summary ---
    try:
        if len(filtered_reviews_df) > 0:
            sentiment_counts = filtered_reviews_df['review_sentiment'].value_counts().reset_index()
            sentiment_counts.columns = ['Sentiment', 'Count']
            
            colors = {'Positive': '#00d084', 'Negative': '#ff6b6b', 'Neutral': '#a29bfe'}
            sentiment_colors = [colors.get(s, '#95a5a6') for s in sentiment_counts['Sentiment']]
            
            fig_sentiment = go.Figure(data=[go.Pie(
                labels=sentiment_counts['Sentiment'],
                values=sentiment_counts['Count'],
                hole=0.4,
                marker=dict(colors=sentiment_colors),
                textinfo='label+percent',
                textfont=dict(size=14)
            )])
            fig_sentiment.update_layout(
                title=dict(text=f"💭 User Sentiment{title_suffix}", font=dict(size=18, color='#2c3e50')),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                showlegend=True,
                margin=dict(l=20, r=20, t=60, b=40),
                height=400
            )
        else:
            fig_sentiment = go.Figure()
    except Exception as e:
        print(f"Error creating sentiment chart: {e}")
        fig_sentiment = go.Figure()

    # --- Category Distribution ---
    try:
        if not selected_category:
            top_categories = filtered_metrics_df['category_name'].value_counts().head(10).reset_index()
            top_categories.columns = ['Category', 'Count']
            
            fig_category = go.Figure(go.Bar(
                x=top_categories['Category'],
                y=top_categories['Count'],
                marker=dict(
                    color=top_categories['Count'],
                    colorscale='Blues',
                    showscale=False
                ),
                text=top_categories['Count'],
                textposition='auto'
            ))
            fig_category.update_layout(
                title=dict(text="📁 Top 10 Categories", font=dict(size=18, color='#2c3e50')),
                xaxis_title="",
                yaxis_title="Number of Apps",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=20, r=20, t=60, b=40),
                height=400
            )
            fig_category.update_xaxes(tickangle=-45)
        else:
            # Show price distribution for selected category
            price_dist = filtered_metrics_df.groupby('price')['app_name'].count().reset_index()
            price_dist.columns = ['Price', 'Count']
            
            fig_category = go.Figure(go.Bar(
                x=price_dist['Price'],
                y=price_dist['Count'],
                marker=dict(color='#667eea'),
                text=price_dist['Count'],
                textposition='auto'
            ))
            fig_category.update_layout(
                title=dict(text=f"💰 Price Distribution{title_suffix}", font=dict(size=18, color='#2c3e50')),
                xaxis_title="Price",
                yaxis_title="Number of Apps",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=20, r=20, t=60, b=40),
                height=400
            )
    except Exception as e:
        print(f"Error creating category chart: {e}")
        fig_category = go.Figure()

    return kpi_cards, fig_rated, fig_scatter, fig_sentiment, fig_category

# --- 6. Run Server ---
if __name__ == '__main__':
    print("Starting Dash server...")
    app.run_server(debug=True, host='0.0.0.0', port=8050)
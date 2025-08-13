# app.py - Complete Working Flask Application for Maonas Database - UPDATED FOR RAILWAY

from flask import Flask, render_template, request, jsonify, send_file, request, session, redirect, url_for, render_template_string, flash
import pandas as pd
import mysql.connector
from mysql.connector import Error
import json
import os
import numpy as np
from datetime import datetime, timedelta
import networkx as nx
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import io
import base64
import warnings
import json
import plotly.graph_objects as go
import plotly.utils
from plotly.subplots import make_subplots
import plotly.express as px
warnings.filterwarnings('ignore')
from datetime import timedelta

# ADD: Load environment variables
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
# UPDATE: Use environment variable for secret key
app.secret_key = os.environ.get('SECRET_KEY', 'maonas-database-2024-beta-secret')
app.permanent_session_lifetime = timedelta(hours=24) 
# ADD Beta Testing Configuration right after:
BETA_MODE = os.environ.get('BETA_MODE', 'true').lower() == 'true'
EXPORTS_ENABLED = not BETA_MODE  

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'database': 'maonas',
    'user': 'root',
    'password': 'Aa77aque.'
}

# Table Display Names Mapping
TABLE_DISPLAY_NAMES = {
    'imt': 'Individuals',
    'legal_acts': 'Legal Acts',
    'vessel': 'Vessels',
    'org': 'Organizations',
    'good_price': "Goods' Prices",
    'exchange_rates': 'Exchange Rates',
    'equivalency': 'Equivalencies'
}

# Updated Database Connection Function
def get_db_connection():
    """Get database connection - supports both MySQL (local) and PostgreSQL (Railway)"""
    try:
        # Check if we're using PostgreSQL (Railway production)
        database_url = os.environ.get('DATABASE_URL')
        if database_url and database_url.startswith('postgresql'):
            # Use psycopg2 for PostgreSQL
            import psycopg2
            from urllib.parse import urlparse
            
            url = urlparse(database_url)
            connection = psycopg2.connect(
                host=url.hostname,
                port=url.port,
                database=url.path[1:],
                user=url.username,
                password=url.password
            )
            return connection
        else:
            # Use MySQL connector for local development
            DB_CONFIG = {
                'host': os.environ.get('DB_HOST', 'localhost'),
                'port': int(os.environ.get('DB_PORT', 3306)),
                'database': os.environ.get('DB_DATABASE', 'maonas'),  # Using your .env variable name
                'user': os.environ.get('DB_USER', 'root'),
                'password': os.environ.get('DB_PASSWORD', 'Aa77aque.')
            }
            return mysql.connector.connect(**DB_CONFIG)
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def execute_query(query, params=None):
    """Execute query and return results as DataFrame - supports MySQL and PostgreSQL"""
    connection = get_db_connection()
    if not connection:
        return pd.DataFrame()
    
    try:
        print(f"EXECUTING QUERY: {query}")
        df = pd.read_sql(query, connection, params=params)
        print(f"QUERY RESULT: {len(df)} rows returned")
        return df
    except Exception as e:
        print(f"Query execution error: {e}")
        return pd.DataFrame()
    finally:
        if connection:
            connection.close()

# ADD: Beta access protection
from functools import wraps

def beta_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if user is already authenticated in this session
        if session.get('beta_authenticated'):
            return f(*args, **kwargs)
        
        # Check if beta_key is provided in the URL (first time only)
        beta_key = request.args.get('beta_key')
         # Get all valid passwords (support multiple passwords)
        valid_passwords = os.environ.get('BETA_PASSWORD', '').split(',')
        valid_passwords = [pwd.strip() for pwd in valid_passwords if pwd.strip()]
        
        if beta_key in valid_passwords:
            session['beta_authenticated'] = True
            session.permanent = True
            # Store which password was used (optional - for tracking)
            session['beta_password_used'] = beta_key
            return redirect(request.url.split('?')[0])
        # If not authenticated, show login page
        return render_template_string('''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Maonas Database - Beta Access</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { 
            font-family: 'Times New Roman', serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .beta-card {
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            padding: 3rem;
            max-width: 400px;
            width: 100%;
            text-align: center;
        }
        .beta-logo {
            width: 80px;
            height: 80px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 50%;
            margin: 0 auto 2rem;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 2rem;
        }
        .form-control {
            border-radius: 25px;
            padding: 12px 20px;
            border: 2px solid #e9ecef;
            margin-bottom: 1rem;
        }
        .form-control:focus {
            border-color: #667eea;
            box-shadow: 0 0 0 0.2rem rgba(102, 126, 234, 0.25);
        }
        .btn-beta {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border: none;
            border-radius: 25px;
            padding: 12px 30px;
            color: white;
            font-weight: 600;
            width: 100%;
            transition: transform 0.2s;
        }
        .btn-beta:hover {
            transform: translateY(-2px);
            color: white;
        }
    </style>
</head>
<body>
    <div class="beta-card">
        <div class="beta-logo">
            🚢
        </div>
        <h2 class="mb-3">Maonas Database</h2>
        <h4 class="text-muted mb-4">Beta Access Required</h4>
        
        {% if request.args.get('beta_key') %}
        <div class="alert alert-danger">
            Invalid access key. Please try again.
        </div>
        {% endif %}
        
        <form method="GET" action="{{ request.url.split('?')[0] }}">
            <input type="password" 
                   name="beta_key" 
                   class="form-control" 
                   placeholder="Enter beta access key"
                   required
                   autofocus>
            <button type="submit" class="btn btn-beta">
                Access Beta System
            </button>
        </form>
        
        <div class="mt-4">
            <small class="text-muted">
                Contact the research team for beta access credentials<br>
                <strong>One-time entry:</strong> Access persists throughout your session
            </small>
        </div>
    </div>
</body>
</html>
        ''')
    
    return decorated_function
# Export control decorator
def exports_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not EXPORTS_ENABLED:
            return jsonify({
                'error': 'Export functionality is disabled during beta testing',
                'message': 'Export features will be available in the full release',
                'beta_mode': True
            }), 403
        return f(*args, **kwargs)
    return decorated_function

# Context processor for templates
@app.context_processor
def inject_beta_config():
    return {
        'BETA_MODE': BETA_MODE,
        'EXPORTS_ENABLED': EXPORTS_ENABLED
    }


# ============================================================================
# MAIN ROUTES
# ============================================================================   
# ============================================================================
# MAIN ROUTES
# ============================================================================
# Add this route to handle logout (optional)
@app.route('/beta/logout')
def beta_logout():
    session.pop('beta_authenticated', None)
    return redirect(url_for('home'))

@app.route('/')
@beta_required
def home():
    return render_template('home.html')

@app.route('/database')
@beta_required
def database_overview():
    tables = TABLE_DISPLAY_NAMES
    return render_template('database_overview.html', tables=tables)

@app.route('/team')
@beta_required
def team():
    return render_template('team.html')

@app.route('/analysis')
@beta_required
def analysis_tools():
    return render_template('analysis_tools.html')
# ============================================================================
# DATABASE API ROUTES
# ============================================================================

@app.route('/api/table/<table_name>')
def get_table_data(table_name):
    """Enhanced API endpoint to get table data with sorting and comprehensive search"""
    search = request.args.get('search', '')
    sort_column = request.args.get('sort_column', '')
    sort_order = request.args.get('sort_order', 'asc')
    
    if table_name not in TABLE_DISPLAY_NAMES:
        return jsonify({'error': 'Table not found'}), 404
    
    try:
        # Enhanced queries with date formatting and foreign key resolution
        if table_name == 'imt':
            base_query = """
                SELECT 
                    CONCAT(COALESCE(FiName, ''), ' ', COALESCE(LaName1, '')) as full_name,
                    FiName as first_name,
                    LaName1 as last_name,
                    birth_place,
                    birth_year,
                    death_year,
                    description
                FROM imt 
                WHERE i_id IS NOT NULL
            """
            search_fields = ['FiName', 'LaName1', 'birth_place', 'description']
        
        elif table_name == 'legal_acts':
            base_query = """
                SELECT 
                    t.name as type,
                    DATE(la.date) as date,
                    l.name as language,
                    CONCAT(COALESCE(n.FiName, ''), ' ', COALESCE(n.LaName1, '')) as notary_full_name,
                    g.g_name as location,
                    la.value,
                    la.description
                FROM legal_acts la
                LEFT JOIN type t ON la.type = t.t_id
                LEFT JOIN language l ON la.language = l.id
                LEFT JOIN imt n ON la.notary = n.i_id
                LEFT JOIN gid g ON la.a_gid = g.g_id
                WHERE la.la_id IS NOT NULL
            """
            search_fields = ['t.name', 'l.name', 'n.FiName', 'n.LaName1', 'g.g_name', 'la.description']
        
        elif table_name == 'good_price':
            base_query = """
                SELECT 
                    good as good_name,
                    Currency as currency,
                    unit,
                    Rate as rate,
                    DATE(StartDate) as start_date,
                    DATE(EndDate) as end_date,
                    Notes as notes
                FROM good_price 
                WHERE gp_id IS NOT NULL
            """
            search_fields = ['good', 'Currency', 'unit', 'Notes']
        
        else:
            base_query = f"SELECT * FROM {table_name}"
            search_fields = []
        
        # Add search conditions if provided
        if search and search_fields:
            search_conditions = []
            for field in search_fields:
                search_conditions.append(f"CAST({field} AS CHAR) LIKE '%{search}%'")
            
            search_clause = " OR ".join(search_conditions)
            base_query += f" AND ({search_clause})"
        
        # Add sorting if provided
        if sort_column:
            order_direction = "DESC" if sort_order.lower() == 'desc' else "ASC"
            base_query += f" ORDER BY {sort_column} {order_direction}"
        
        df = execute_query(base_query)
        
        if df.empty:
            return jsonify({
                'columns': [],
                'data': [],
                'total_records': 0
            })
        
        # Clean up data for JSON serialization
        df = df.replace([float('nan'), float('inf'), float('-inf')], None)
        df = df.where(pd.notnull(df), None)
        
        data = {
            'columns': df.columns.tolist(),
            'data': df.to_dict('records'),
            'total_records': len(df)
        }
        
        return jsonify(data)
        
    except Exception as e:
        error_msg = f"Error querying table {table_name}: {str(e)}"
        print(f"ERROR: {error_msg}")
        return jsonify({'error': error_msg}), 500

@app.route('/api/database/stats')
def get_database_stats():
    """Get basic database statistics"""
    try:
        stats = {}
        for table_key in TABLE_DISPLAY_NAMES.keys():
            try:
                count_query = f"SELECT COUNT(*) as count FROM {table_key}"
                result = execute_query(count_query)
                stats[table_key] = result.iloc[0]['count'] if not result.empty else 0
            except:
                stats[table_key] = 0
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# NETWORK ANALYSIS FUNCTIONS - CORRECTED VERSION
# ============================================================================
# PROPER FIXES FOR APP.PY - Add these to your existing app.py

# 1. FIX THE build_network_from_db function with proper goods filtering
def build_network_from_db(network_type='global', start_date=None, end_date=None, individual_id=None, good_id=None):
    """Build NetworkX graph from database relationships - CORRECTED VERSION"""
    G = nx.Graph()
    
    try:
        print(f"Building {network_type} network...")
        if good_id:
            print(f"Filtering by good_id: {good_id}")
        
        # BUILD QUERIES WITH PROPER GOODS FILTERING STRUCTURE
        if good_id:
            # QUERIES WITH GOODS FILTERING - Using your correct pattern
            
            # Query 1: la_party_1 to la_party_1 connections WITH GOODS
            query1 = """
            SELECT d1.i_id as source,
                   d2.i_id as target,
                   la.date as timestamp
            FROM (SELECT * FROM la_party_1) as d1
            INNER JOIN (SELECT * FROM la_party_1) as d2 
                ON d1.la_id = d2.la_id AND d1.i_id < d2.i_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            INNER JOIN la_gp as lgp ON la.la_id = lgp.la_id
            INNER JOIN good_price as gp ON lgp.gp_id = gp.gp_id
            WHERE d1.i_id IS NOT NULL AND d2.i_id IS NOT NULL 
            AND gp.good = %s
            """
            
            # Query 2: la_party_2 to la_party_2 connections WITH GOODS
            query2 = """
            SELECT d1.i_id as source,
                   d2.i_id as target,
                   la.date as timestamp
            FROM (SELECT * FROM la_party_2) as d1
            INNER JOIN (SELECT * FROM la_party_2) as d2 
                ON d1.la_id = d2.la_id AND d1.i_id < d2.i_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            INNER JOIN la_gp as lgp ON la.la_id = lgp.la_id
            INNER JOIN good_price as gp ON lgp.gp_id = gp.gp_id
            WHERE d1.i_id IS NOT NULL AND d2.i_id IS NOT NULL 
            AND gp.good = %s
            """
            
            # Query 3: la_mentions to la_mentions connections WITH GOODS (your example)
            query3 = """
            SELECT d1.i_id as source,
                   d2.i_id as target,
                   la.date as timestamp
            FROM (SELECT * FROM la_mentions) as d1
            INNER JOIN (SELECT * FROM la_mentions) as d2 
                ON d1.la_id = d2.la_id AND d1.i_id < d2.i_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            INNER JOIN la_gp as lgp ON la.la_id = lgp.la_id
            INNER JOIN good_price as gp ON lgp.gp_id = gp.gp_id
            WHERE d1.i_id IS NOT NULL AND d2.i_id IS NOT NULL 
            AND gp.good = %s
            """
            
            # Query 4: la_party_1 to la_party_2 connections WITH GOODS
            query4 = """
            SELECT d1.i_id as source,
                   d2.i_id as target,
                   la.date as timestamp
            FROM la_party_1 as d1
            LEFT JOIN la_party_2 as d2 ON d1.la_id = d2.la_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            INNER JOIN la_gp as lgp ON la.la_id = lgp.la_id
            INNER JOIN good_price as gp ON lgp.gp_id = gp.gp_id
            WHERE d1.i_id IS NOT NULL AND d2.i_id IS NOT NULL 
            AND gp.good = %s
            """
            
            # Query 5: la_party_1 to la_mentions connections WITH GOODS
            query5 = """
            SELECT d1.i_id as source,
                   d3.i_id as target,
                   la.date as timestamp
            FROM la_party_1 as d1
            LEFT JOIN la_mentions as d3 ON d1.la_id = d3.la_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            INNER JOIN la_gp as lgp ON la.la_id = lgp.la_id
            INNER JOIN good_price as gp ON lgp.gp_id = gp.gp_id
            WHERE d1.i_id IS NOT NULL AND d3.i_id IS NOT NULL 
            AND gp.good = %s
            """
            
            # Query 6: la_party_2 to la_mentions connections WITH GOODS
            query6 = """
            SELECT d2.i_id as source,
                   d3.i_id as target,
                   la.date as timestamp
            FROM la_party_2 as d2
            LEFT JOIN la_mentions as d3 ON d2.la_id = d3.la_id
            INNER JOIN legal_acts as la ON d2.la_id = la.la_id
            INNER JOIN la_gp as lgp ON la.la_id = lgp.la_id
            INNER JOIN good_price as gp ON lgp.gp_id = gp.gp_id
            WHERE d2.i_id IS NOT NULL AND d3.i_id IS NOT NULL 
            AND gp.good = %s
            """
            
            # All queries need the good_id parameter
            all_queries = [query1, query2, query3, query4, query5, query6]
            query_params = [good_id] * 6
            
        else:
            # ORIGINAL QUERIES WITHOUT GOODS FILTERING (keep your working structure)
            query1 = """
            SELECT d1.i_id as source,
                   d2.i_id as target,
                   la.date as timestamp
            FROM (SELECT * FROM la_party_1) as d1
            INNER JOIN (SELECT * FROM la_party_1) as d2 
                ON d1.la_id = d2.la_id AND d1.i_id < d2.i_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            WHERE d1.i_id IS NOT NULL AND d2.i_id IS NOT NULL
            """
            
            query2 = """
            SELECT d1.i_id as source,
                   d2.i_id as target,
                   la.date as timestamp
            FROM (SELECT * FROM la_party_2) as d1
            INNER JOIN (SELECT * FROM la_party_2) as d2 
                ON d1.la_id = d2.la_id AND d1.i_id < d2.i_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            WHERE d1.i_id IS NOT NULL AND d2.i_id IS NOT NULL
            """
            
            query3 = """
            SELECT d1.i_id as source,
                   d2.i_id as target,
                   la.date as timestamp
            FROM (SELECT * FROM la_mentions) as d1
            INNER JOIN (SELECT * FROM la_mentions) as d2 
                ON d1.la_id = d2.la_id AND d1.i_id < d2.i_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            WHERE d1.i_id IS NOT NULL AND d2.i_id IS NOT NULL
            """
            
            query4 = """
            SELECT d1.i_id as source,
                   d2.i_id as target,
                   la.date as timestamp
            FROM la_party_1 as d1
            LEFT JOIN la_party_2 as d2 ON d1.la_id = d2.la_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            WHERE d1.i_id IS NOT NULL AND d2.i_id IS NOT NULL
            """
            
            query5 = """
            SELECT d1.i_id as source,
                   d3.i_id as target,
                   la.date as timestamp
            FROM la_party_1 as d1
            LEFT JOIN la_mentions as d3 ON d1.la_id = d3.la_id
            INNER JOIN legal_acts as la ON d1.la_id = la.la_id
            WHERE d1.i_id IS NOT NULL AND d3.i_id IS NOT NULL
            """
            
            query6 = """
            SELECT d2.i_id as source,
                   d3.i_id as target,
                   la.date as timestamp
            FROM la_party_2 as d2
            LEFT JOIN la_mentions as d3 ON d2.la_id = d3.la_id
            INNER JOIN legal_acts as la ON d2.la_id = la.la_id
            WHERE d2.i_id IS NOT NULL AND d3.i_id IS NOT NULL
            """
            
            # No parameters for non-goods queries
            all_queries = [query1, query2, query3, query4, query5, query6]
            query_params = [None] * 6
        
        # Add date filtering to all queries if provided
        if start_date and end_date:
            date_filter = f" AND la.date BETWEEN '{start_date}' AND '{end_date}'"
            all_queries = [q + date_filter for q in all_queries]
        
        # Add individual filtering if provided
        if individual_id and network_type == 'individual_centered':
            individual_filter = f" AND (d1.i_id = '{individual_id}' OR d2.i_id = '{individual_id}' OR d3.i_id = '{individual_id}')"
            # Apply individual filter carefully to each query
            for i, query in enumerate(all_queries):
                if 'd3' in query:
                    all_queries[i] = query + individual_filter
                else:
                    all_queries[i] = query + individual_filter.replace(' OR d3.i_id', '')
        
        # Execute all queries and combine results
        all_edges = []
        
        for i, (query, params) in enumerate(zip(all_queries, query_params), 1):
            try:
                print(f"Executing query {i}...")
                
                # Execute with or without parameters
                if params:
                    df = execute_query(query, params=[params])
                else:
                    df = execute_query(query)
                
                if not df.empty:
                    print(f"Query {i} returned {len(df)} edges")
                    all_edges.append(df)
                else:
                    print(f"Query {i} returned no edges")
            except Exception as e:
                print(f"Error in query {i}: {e}")
                continue
        
        # Combine all edge data - same as your original
        if all_edges:
            combined_df = pd.concat(all_edges, ignore_index=True)
            print(f"Combined dataframe has {len(combined_df)} total edges")
            
            # Remove duplicates and aggregate by source-target pairs
            edge_weights = combined_df.groupby(['source', 'target']).size().reset_index(name='frequency')
            print(f"After aggregation: {len(edge_weights)} unique edges")
            
            # Add edges to graph
            for _, row in edge_weights.iterrows():
                if pd.notna(row['source']) and pd.notna(row['target']) and row['source'] != row['target']:
                    G.add_edge(str(row['source']), str(row['target']), 
                             weight=int(row['frequency']))
        else:
            print("No edges found in any query")
        
        # Add node labels
        G = add_node_labels_global(G)
        
        print(f"Final network: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        return G
        
    except Exception as e:
        print(f"Error building network: {e}")
        import traceback
        traceback.print_exc()
        return nx.Graph()

def add_node_labels_global(G):
    """Add proper node labels for both individuals and organizations"""
    try:
        # Get individual names
        individuals_query = """
        SELECT 
            i_id,
            CONCAT(COALESCE(FiName, ''), ' ', COALESCE(LaName1, '')) as full_name,
            FiName,
            LaName1
        FROM imt 
        WHERE i_id IS NOT NULL
        """
        individuals_df = execute_query(individuals_query)
        
        # Get organization names
        organizations_query = """
        SELECT 
            o_id,
            o_name
        FROM org 
        WHERE o_id IS NOT NULL AND o_name IS NOT NULL
        """
        organizations_df = execute_query(organizations_query)
        
        # Add individual node attributes
        if not individuals_df.empty:
            for _, row in individuals_df.iterrows():
                node_id = str(row['i_id'])
                if node_id in G.nodes():
                    full_name = str(row['full_name']).strip()
                    if full_name == ' ' or full_name == 'None None':
                        if pd.notna(row['FiName']):
                            full_name = str(row['FiName'])
                        elif pd.notna(row['LaName1']):
                            full_name = str(row['LaName1'])
                        else:
                            full_name = node_id
                    
                    G.nodes[node_id]['label'] = full_name
                    G.nodes[node_id]['type'] = 'individual'
        
        # Add organization node attributes
        if not organizations_df.empty:
            for _, row in organizations_df.iterrows():
                # Check both formats: with and without ORG_ prefix
                node_id_plain = str(row['o_id'])
                node_id_prefixed = f"ORG_{row['o_id']}"
                
                for node_id in [node_id_plain, node_id_prefixed]:
                    if node_id in G.nodes():
                        G.nodes[node_id]['label'] = str(row['o_name'])
                        G.nodes[node_id]['type'] = 'organization'
        
        # Set default labels for nodes without data
        for node in G.nodes():
            if 'label' not in G.nodes[node]:
                G.nodes[node]['label'] = node
                G.nodes[node]['type'] = 'unknown'
        
        return G
        
    except Exception as e:
        print(f"Error adding node labels: {e}")
        return G

def filter_network_by_connections(G, min_connections=1):
    """Filter network to only include nodes with minimum number of connections"""
    if min_connections <= 1:
        return G
    
    try:
        print(f"Filtering network: minimum {min_connections} connections")
        print(f"Original network: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        
        # Identify nodes to remove (those with fewer than min_connections)
        nodes_to_remove = []
        for node in G.nodes():
            if G.degree(node) < min_connections:
                nodes_to_remove.append(node)
        
        print(f"Removing {len(nodes_to_remove)} nodes with < {min_connections} connections")
        
        # Create filtered graph
        G_filtered = G.copy()
        G_filtered.remove_nodes_from(nodes_to_remove)
        
        print(f"Filtered network: {G_filtered.number_of_nodes()} nodes, {G_filtered.number_of_edges()} edges")
        
        return G_filtered
        
    except Exception as e:
        print(f"Error filtering network: {e}")
        return G
# REPLACE the calculate_network_metrics function in your app.py with this enhanced version:

def calculate_network_metrics(G, measures):
    """Calculate network metrics - ENHANCED VERSION with better connectivity handling"""
    results = {}
    
    if G.number_of_nodes() == 0:
        return {'error': 'Network is empty - no connections found in database'}
    
    try:
        # Enhanced network info with component analysis
        is_connected = nx.is_connected(G)
        components = list(nx.connected_components(G))
        largest_component = max(components, key=len) if components else set()
        
        results['network_info'] = {
            'num_nodes': G.number_of_nodes(),
            'num_edges': G.number_of_edges(),
            'is_connected': is_connected,
            'num_components': len(components),
            'largest_component_size': len(largest_component),
            'connectivity_ratio': len(largest_component) / G.number_of_nodes() if G.number_of_nodes() > 0 else 0
        }
        
        # If network is not connected, work with the largest component for certain measures
        main_graph = G
        if not is_connected and len(largest_component) > 1:
            main_graph = G.subgraph(largest_component).copy()
            print(f"Using largest component ({len(largest_component)} nodes) for connectivity-dependent measures")
        
        # Get node labels for top results
        def get_node_label(node_id):
            return G.nodes[node_id].get('label', node_id)
        
        # Centrality measures
        if 'degree' in measures:
            try:
                degree_centrality = nx.degree_centrality(G)
                top_nodes = dict(sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:10])
                top_labeled = {get_node_label(node): value for node, value in top_nodes.items()}
                
                results['degree_centrality'] = {
                    'top_nodes': top_labeled,
                    'average': float(np.mean(list(degree_centrality.values()))),
                    'std': float(np.std(list(degree_centrality.values())))
                }
            except Exception as e:
                results['degree_centrality'] = {'error': str(e)}
        
        if 'betweenness' in measures:
            try:
                betweenness_centrality = nx.betweenness_centrality(G)
                top_nodes = dict(sorted(betweenness_centrality.items(), key=lambda x: x[1], reverse=True)[:10])
                top_labeled = {get_node_label(node): value for node, value in top_nodes.items()}
                
                results['betweenness_centrality'] = {
                    'top_nodes': top_labeled,
                    'average': float(np.mean(list(betweenness_centrality.values()))),
                    'std': float(np.std(list(betweenness_centrality.values())))
                }
            except Exception as e:
                results['betweenness_centrality'] = {'error': str(e)}
        
        if 'closeness' in measures:
            try:
                if is_connected:
                    closeness_centrality = nx.closeness_centrality(G)
                    top_nodes = dict(sorted(closeness_centrality.items(), key=lambda x: x[1], reverse=True)[:10])
                    top_labeled = {get_node_label(node): value for node, value in top_nodes.items()}
                    
                    results['closeness_centrality'] = {
                        'top_nodes': top_labeled,
                        'average': float(np.mean(list(closeness_centrality.values()))),
                        'std': float(np.std(list(closeness_centrality.values()))),
                        'note': 'Calculated on full connected network'
                    }
                else:
                    # Calculate closeness for the largest component
                    if len(largest_component) > 1:
                        closeness_centrality = nx.closeness_centrality(main_graph)
                        top_nodes = dict(sorted(closeness_centrality.items(), key=lambda x: x[1], reverse=True)[:10])
                        top_labeled = {get_node_label(node): value for node, value in top_nodes.items()}
                        
                        results['closeness_centrality'] = {
                            'top_nodes': top_labeled,
                            'average': float(np.mean(list(closeness_centrality.values()))),
                            'std': float(np.std(list(closeness_centrality.values()))),
                            'note': f'Calculated on largest component ({len(largest_component)} nodes)'
                        }
                    else:
                        results['closeness_centrality'] = {'error': 'Network too fragmented for closeness centrality'}
            except Exception as e:
                results['closeness_centrality'] = {'error': str(e)}
        
        if 'eigenvector' in measures:
            try:
                eigenvector_centrality = nx.eigenvector_centrality(G, max_iter=1000)
                top_nodes = dict(sorted(eigenvector_centrality.items(), key=lambda x: x[1], reverse=True)[:10])
                top_labeled = {get_node_label(node): value for node, value in top_nodes.items()}
                
                results['eigenvector_centrality'] = {
                    'top_nodes': top_labeled,
                    'average': float(np.mean(list(eigenvector_centrality.values()))),
                    'std': float(np.std(list(eigenvector_centrality.values())))
                }
            except Exception as e:
                results['eigenvector_centrality'] = {'error': f'Could not converge: {str(e)}'}
        
        # Network properties
        if 'density' in measures:
            results['density'] = float(nx.density(G))
        
        if 'modularity' in measures:
            try:
                # Use the appropriate graph for community detection
                community_graph = main_graph if not is_connected and len(largest_component) > 2 else G
                
                if community_graph.number_of_nodes() > 2:
                    communities = nx.community.greedy_modularity_communities(community_graph)
                    modularity = nx.community.modularity(community_graph, communities)
                    
                    # Get community details
                    community_details = []
                    for i, community in enumerate(communities):
                        community_names = [get_node_label(node) for node in list(community)[:5]]  # Top 5 members
                        community_details.append({
                            'id': i,
                            'size': len(community),
                            'members_sample': community_names
                        })
                    
                    results['modularity'] = {
                        'value': float(modularity),
                        'num_communities': len(communities),
                        'community_sizes': [len(c) for c in communities],
                        'communities': community_details,
                        'note': f'Calculated on {"largest component" if not is_connected else "full network"}'
                    }
                else:
                    results['modularity'] = {'error': 'Network too small for meaningful community detection'}
            except Exception as e:
                results['modularity'] = {'error': str(e)}
        
        if 'triangles' in measures:
            try:
                triangles = nx.triangles(G)
                total_triangles = sum(triangles.values()) // 3
                top_triangle_nodes = dict(sorted(triangles.items(), key=lambda x: x[1], reverse=True)[:10])
                top_labeled = {get_node_label(node): value for node, value in top_triangle_nodes.items()}
                
                results['triangles'] = {
                    'total_count': total_triangles,
                    'average_per_node': float(np.mean(list(triangles.values()))),
                    'top_nodes': top_labeled
                }
            except Exception as e:
                results['triangles'] = {'error': str(e)}
        
        return results
        
    except Exception as e:
        return {'error': f'Error calculating metrics: {str(e)}'}

# ADD this debug function to help analyze connectivity issues:

@app.route('/api/debug/connectivity-analysis')
def debug_connectivity_analysis():
    """Detailed connectivity analysis to understand network structure"""
    try:
        # Build a small sample network to analyze
        G = build_network_from_db('global')
        
        if G.number_of_nodes() == 0:
            return jsonify({'error': 'No network found'})
        
        # Detailed connectivity analysis
        is_connected = nx.is_connected(G)
        components = list(nx.connected_components(G))
        
        analysis = {
            'basic_info': {
                'total_nodes': G.number_of_nodes(),
                'total_edges': G.number_of_edges(),
                'is_connected': is_connected,
                'num_components': len(components)
            },
            'component_analysis': [],
            'connectivity_issues': []
        }
        
        # Analyze each component
        for i, component in enumerate(components):
            component_info = {
                'component_id': i,
                'size': len(component),
                'sample_nodes': [G.nodes[node].get('label', node) for node in list(component)[:5]]
            }
            analysis['component_analysis'].append(component_info)
        
        # Check for common connectivity issues
        if not is_connected:
            largest_component = max(components, key=len)
            analysis['connectivity_issues'].append(
                f"Network has {len(components)} separate components. "
                f"Largest has {len(largest_component)} nodes "
                f"({len(largest_component)/G.number_of_nodes()*100:.1f}% of total)."
            )
            
            # Check for isolated nodes
            isolated_nodes = [node for node in G.nodes() if G.degree(node) == 0]
            if isolated_nodes:
                analysis['connectivity_issues'].append(
                    f"Found {len(isolated_nodes)} isolated nodes with no connections"
                )
        
        # Sample some edges to verify they make sense
        sample_edges = []
        for edge in list(G.edges())[:10]:
            source_label = G.nodes[edge[0]].get('label', edge[0])
            target_label = G.nodes[edge[1]].get('label', edge[1])
            weight = G[edge[0]][edge[1]].get('weight', 1)
            sample_edges.append({
                'source': source_label,
                'target': target_label,
                'weight': weight
            })
        
        analysis['sample_edges'] = sample_edges
        
        return jsonify(analysis)
        
    except Exception as e:
        import traceback
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        })

# UPDATE THE EGO NETWORK FUNCTIONS TO ACCEPT good_id
def build_individual_ego_network(individual_id, start_date=None, end_date=None, good_id=None):
    """Build ego network for a specific individual with optional goods filtering"""
    G = nx.Graph()
    
    try:
        # Build global network first with goods filtering
        global_G = build_network_from_db('global', start_date, end_date, None, good_id)
        
        if individual_id not in global_G.nodes():
            return nx.Graph()
        
        # Get ego network (individual + neighbors + connections between neighbors)
        ego_nodes = [individual_id] + list(global_G.neighbors(individual_id))
        G = global_G.subgraph(ego_nodes).copy()
        
        return G
        
    except Exception as e:
        print(f"Error building ego network: {e}")
        return nx.Graph()

def build_individual_direct_network(individual_id, start_date=None, end_date=None, good_id=None):
    """Build network with only direct connections to the individual with optional goods filtering"""
    G = nx.Graph()
    
    try:
        # Build global network first with goods filtering
        global_G = build_network_from_db('global', start_date, end_date, None, good_id)
        
        if individual_id not in global_G.nodes():
            return nx.Graph()
        
        # Get only direct connections (star network)
        direct_neighbors = list(global_G.neighbors(individual_id))
        G.add_node(individual_id, **global_G.nodes[individual_id])
        
        for neighbor in direct_neighbors:
            G.add_node(neighbor, **global_G.nodes[neighbor])
            G.add_edge(individual_id, neighbor, **global_G.edges[individual_id, neighbor])
        
        return G
        
    except Exception as e:
        print(f"Error building direct network: {e}")
        return nx.Graph()

# ============================================================================
# ECONOMIC ANALYSIS FUNCTIONS
# ============================================================================

def get_price_data_from_db(goods=None, start_date=None, end_date=None):
    """Get price data from database"""
    
    base_query = """
    SELECT 
        gp.good as good_name,
        gp.Rate as price,
        gp.Currency as currency,
        gp.StartDate as date,
        gp.Notes as notes
    FROM good_price gp
    WHERE gp.Rate IS NOT NULL AND gp.Rate > 0
    """
    
    conditions = []
    
    if goods and 'all' not in goods:
        goods_str = "', '".join([str(g) for g in goods])
        conditions.append(f"gp.good IN ('{goods_str}')")
    
    if start_date and end_date:
        conditions.append(f"gp.StartDate BETWEEN '{start_date}' AND '{end_date}'")
    
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
    
    base_query += " ORDER BY gp.StartDate"
    
    return execute_query(base_query)

def calculate_descriptive_stats(price_data):
    """Calculate descriptive statistics for price data"""
    if price_data.empty:
        return {}
    
    stats = {}
    
    for good in price_data['good_name'].unique():
        good_data = price_data[price_data['good_name'] == good]['price']
        
        if len(good_data) > 0:
            stats[good] = {
                'count': int(len(good_data)),
                'mean': float(good_data.mean()),
                'median': float(good_data.median()),
                'std': float(good_data.std()) if len(good_data) > 1 else 0.0,
                'min': float(good_data.min()),
                'max': float(good_data.max()),
                'range': float(good_data.max() - good_data.min())
            }
    
    return stats

def calculate_volatility(price_data):
    """Calculate price volatility measures"""
    if price_data.empty:
        return {}
    
    volatility = {}
    
    for good in price_data['good_name'].unique():
        good_data = price_data[price_data['good_name'] == good].copy()
        good_data = good_data.sort_values('date')
        
        if len(good_data) > 1:
            # Calculate price changes
            good_data['price_change'] = good_data['price'].pct_change()
            good_data = good_data.dropna()
            
            if len(good_data) > 0:
                volatility[good] = {
                    'price_volatility': float(good_data['price_change'].std()),
                    'max_price_change': float(good_data['price_change'].max()),
                    'min_price_change': float(good_data['price_change'].min())
                }
    
    return volatility

def get_network_layout(G, layout_type='spring'):
    """Generate network layout positions"""
    try:
        if layout_type == 'spring':
            pos = nx.spring_layout(G, k=1, iterations=50, seed=42)
        elif layout_type == 'circular':
            pos = nx.circular_layout(G)
        elif layout_type == 'kamada_kawai':
            pos = nx.kamada_kawai_layout(G)
        elif layout_type == 'shell':
            pos = nx.shell_layout(G)
        elif layout_type == 'random':
            pos = nx.random_layout(G, seed=42)
        else:
            pos = nx.spring_layout(G, k=1, iterations=50, seed=42)
        
        return pos
        
    except Exception as e:
        print(f"Layout generation error: {e}")
        # Fallback to simple layout
        return {node: (i % 10, i // 10) for i, node in enumerate(G.nodes())}

def calculate_node_attributes(G, color_by='degree', size_by='degree'):
    """Calculate node attributes for visualization"""
    attributes = {}
    
    # Calculate centrality measures for coloring/sizing
    if color_by == 'degree' or size_by == 'degree':
        degree_centrality = nx.degree_centrality(G)
    
    if color_by == 'betweenness' or size_by == 'betweenness':
        betweenness_centrality = nx.betweenness_centrality(G)
    
    if color_by == 'closeness' or size_by == 'closeness':
        if nx.is_connected(G):
            closeness_centrality = nx.closeness_centrality(G)
        else:
            # Calculate for largest component only
            largest_cc = max(nx.connected_components(G), key=len)
            largest_subgraph = G.subgraph(largest_cc)
            closeness_centrality = nx.closeness_centrality(largest_subgraph)
            # Set 0 for nodes not in largest component
            for node in G.nodes():
                if node not in closeness_centrality:
                    closeness_centrality[node] = 0
    
    if color_by == 'eigenvector' or size_by == 'eigenvector':
        try:
            eigenvector_centrality = nx.eigenvector_centrality(G, max_iter=1000)
        except:
            eigenvector_centrality = {node: 0 for node in G.nodes()}
    
    # Assign attributes to each node
    for node in G.nodes():
        attributes[node] = {
            'label': G.nodes[node].get('label', str(node)),
            'type': G.nodes[node].get('type', 'unknown'),
            'degree': G.degree(node)
        }
        
        # Color value
        if color_by == 'degree':
            attributes[node]['color_value'] = degree_centrality.get(node, 0)
        elif color_by == 'betweenness':
            attributes[node]['color_value'] = betweenness_centrality.get(node, 0)
        elif color_by == 'closeness':
            attributes[node]['color_value'] = closeness_centrality.get(node, 0)
        elif color_by == 'eigenvector':
            attributes[node]['color_value'] = eigenvector_centrality.get(node, 0)
        elif color_by == 'type':
            # Assign numeric values for node types
            type_map = {'individual': 1, 'organization': 2, 'unknown': 0}
            attributes[node]['color_value'] = type_map.get(G.nodes[node].get('type', 'unknown'), 0)
        else:
            attributes[node]['color_value'] = 0
        
        # Size value
        if size_by == 'degree':
            attributes[node]['size_value'] = degree_centrality.get(node, 0)
        elif size_by == 'betweenness':
            attributes[node]['size_value'] = betweenness_centrality.get(node, 0)
        elif size_by == 'closeness':
            attributes[node]['size_value'] = closeness_centrality.get(node, 0)
        elif size_by == 'eigenvector':
            attributes[node]['size_value'] = eigenvector_centrality.get(node, 0)
        else:
            attributes[node]['size_value'] = degree_centrality.get(node, 0)
    
    return attributes

# ============================================================================
# ANALYSIS API ROUTES - UPDATED
# ============================================================================
@app.route('/api/analysis/network-export', methods=['POST'])
@beta_required
@exports_required
def export_network_data():
    """Export network data in various formats"""
    try:
        data = request.get_json()
        
        network_type = data.get('network_type', 'global')
        individual_id = data.get('individual_id')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        min_connections = int(data.get('min_connections', 1))
        export_format = data.get('format', 'csv')
        
        # Build network
        if network_type == 'individual_centered' and individual_id:
            G = build_individual_ego_network(individual_id, start_date, end_date)
        else:
            G = build_network_from_db('global', start_date, end_date)
        
        if G.number_of_nodes() == 0:
            return jsonify({'error': 'No network data to export'})
        
        # Apply filters
        G_filtered = filter_network_by_connections(G, min_connections)
        
        if export_format == 'csv':
            # Export as edge list CSV
            edges_data = []
            for edge in G_filtered.edges(data=True):
                source_label = G_filtered.nodes[edge[0]].get('label', edge[0])
                target_label = G_filtered.nodes[edge[1]].get('label', edge[1])
                weight = edge[2].get('weight', 1)
                
                edges_data.append({
                    'Source_ID': edge[0],
                    'Target_ID': edge[1],
                    'Source_Name': source_label,
                    'Target_Name': target_label,
                    'Weight': weight
                })
            
            # Also create nodes CSV
            nodes_data = []
            for node in G_filtered.nodes(data=True):
                nodes_data.append({
                    'Node_ID': node[0],
                    'Label': node[1].get('label', node[0]),
                    'Type': node[1].get('type', 'unknown'),
                    'Degree': G_filtered.degree(node[0])
                })
            
            edges_df = pd.DataFrame(edges_data)
            nodes_df = pd.DataFrame(nodes_data)
            
            # Create temporary files
            import tempfile
            import zipfile
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.zip', delete=False) as tmp_file:
                with zipfile.ZipFile(tmp_file.name, 'w') as zipf:
                    # Write edges CSV
                    edges_csv = edges_df.to_csv(index=False)
                    zipf.writestr('network_edges.csv', edges_csv)
                    
                    # Write nodes CSV
                    nodes_csv = nodes_df.to_csv(index=False)
                    zipf.writestr('network_nodes.csv', nodes_csv)
                
                return send_file(tmp_file.name, as_attachment=True, 
                               download_name='network_data.zip', mimetype='application/zip')
        
        elif export_format == 'gexf':
            # Export as GEXF (Gephi format)
            import tempfile
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.gexf', delete=False) as tmp_file:
                nx.write_gexf(G_filtered, tmp_file.name)
                return send_file(tmp_file.name, as_attachment=True, 
                               download_name='network.gexf', mimetype='application/xml')
        
        else:
            return jsonify({'error': 'Unsupported export format'})
        
    except Exception as e:
        print(f"Export error: {e}")
        return jsonify({'error': f'Export failed: {str(e)}'}), 500

@app.route('/api/analysis/network-comparison', methods=['POST'])
def network_comparison():
    """Compare networks across different time periods or parameters"""
    try:
        data = request.get_json()
        
        # Get comparison parameters
        period1_start = data.get('period1_start')
        period1_end = data.get('period1_end')
        period2_start = data.get('period2_start')
        period2_end = data.get('period2_end')
        min_connections = int(data.get('min_connections', 1))
        
        # Build networks for both periods
        G1 = build_network_from_db('global', period1_start, period1_end)
        G2 = build_network_from_db('global', period2_start, period2_end)
        
        # Apply filters
        G1_filtered = filter_network_by_connections(G1, min_connections)
        G2_filtered = filter_network_by_connections(G2, min_connections)
        
        # Calculate comparison metrics
        comparison = {
            'period1': {
                'label': f"{period1_start} to {period1_end}",
                'nodes': G1_filtered.number_of_nodes(),
                'edges': G1_filtered.number_of_edges(),
                'density': float(nx.density(G1_filtered)),
                'components': len(list(nx.connected_components(G1_filtered))),
                'avg_clustering': float(nx.average_clustering(G1_filtered))
            },
            'period2': {
                'label': f"{period2_start} to {period2_end}",
                'nodes': G2_filtered.number_of_nodes(),
                'edges': G2_filtered.number_of_edges(),
                'density': float(nx.density(G2_filtered)),
                'components': len(list(nx.connected_components(G2_filtered))),
                'avg_clustering': float(nx.average_clustering(G2_filtered))
            }
        }
        
        # Find common nodes
        common_nodes = set(G1_filtered.nodes()).intersection(set(G2_filtered.nodes()))
        comparison['common_nodes'] = len(common_nodes)
        comparison['node_overlap'] = len(common_nodes) / max(len(G1_filtered.nodes()), len(G2_filtered.nodes())) if max(len(G1_filtered.nodes()), len(G2_filtered.nodes())) > 0 else 0
        
        # Calculate changes
        comparison['changes'] = {
            'nodes_change': comparison['period2']['nodes'] - comparison['period1']['nodes'],
            'edges_change': comparison['period2']['edges'] - comparison['period1']['edges'],
            'density_change': comparison['period2']['density'] - comparison['period1']['density'],
            'components_change': comparison['period2']['components'] - comparison['period1']['components']
        }
        
        return jsonify(comparison)
        
    except Exception as e:
        print(f"Network comparison error: {e}")
        return jsonify({'error': f'Comparison failed: {str(e)}'}), 500

@app.route('/api/analysis/network-stats', methods=['POST'])
def network_statistics():
    """Generate global network statistics - UPDATED with goods support"""
    try:
        data = request.get_json()
        
        # Extract parameters including good_id
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        measures = data.get('measures', [])
        min_connections = int(data.get('min_connections', 1))
        good_id = data.get('good_id')  # NEW PARAMETER
        
        print(f"Building global network, dates={start_date} to {end_date}")
        if good_id:
            print(f"Filtering by good_id: {good_id}")
        print(f"Minimum connections filter: {min_connections}")
        print(f"Requested measures: {measures}")
        
        # Build the global network using the corrected method with goods filtering
        G = build_network_from_db('global', start_date, end_date, None, good_id)
        
        if G.number_of_nodes() == 0:
            error_msg = 'No network data found.'
            if good_id:
                error_msg += f' No legal acts found involving good "{good_id}".'
            return jsonify({'error': error_msg})
        
        # Apply minimum connections filter
        G_filtered = filter_network_by_connections(G, min_connections)
        
        if G_filtered.number_of_nodes() == 0:
            return jsonify({'error': f'No nodes have {min_connections} or more connections. Try lowering the minimum connections filter.'})
        
        # Calculate requested metrics on filtered network
        results = calculate_network_metrics(G_filtered, measures)
        
        # Add filtering information to results
        if 'network_info' in results:
            results['network_info']['min_connections_filter'] = min_connections
            results['network_info']['nodes_removed_by_filter'] = G.number_of_nodes() - G_filtered.number_of_nodes()
            results['network_info']['original_nodes'] = G.number_of_nodes()
            results['network_info']['original_edges'] = G.number_of_edges()
            if good_id:
                results['network_info']['good_filter'] = good_id
        
        return jsonify(results)
        
    except Exception as e:
        print(f"Network analysis error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Network analysis failed: {str(e)}'}), 500

@app.route('/api/search/individuals')
def search_individuals():
    """Search for individuals by name"""
    try:
        search_term = request.args.get('q', '').strip()
        
        if len(search_term) < 2:
            return jsonify({'individuals': []})
        
        # Search in both first and last names
        search_query = """
        SELECT 
            i_id,
            CONCAT(COALESCE(FiName, ''), ' ', COALESCE(LaName1, '')) as full_name,
            FiName,
            LaName1
        FROM imt 
        WHERE (FiName LIKE %s OR LaName1 LIKE %s)
        AND (FiName IS NOT NULL OR LaName1 IS NOT NULL)
        ORDER BY full_name
        LIMIT 20
        """
        
        search_pattern = f"%{search_term}%"
        df = execute_query(search_query, params=[search_pattern, search_pattern])
        
        individuals = []
        if not df.empty:
            for _, row in df.iterrows():
                full_name = str(row['full_name']).strip()
                if full_name == ' ' or full_name == 'None None':
                    if pd.notna(row['FiName']):
                        full_name = str(row['FiName'])
                    elif pd.notna(row['LaName1']):
                        full_name = str(row['LaName1'])
                    else:
                        continue  # Skip if no valid name
                
                individuals.append({
                    'id': row['i_id'],
                    'name': full_name
                })
        
        return jsonify({'individuals': individuals})
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/dates/available-years')
def get_available_years():
    """Get available years from the database for smart date filtering"""
    try:
        # Get min and max years from legal_acts
        years_query = """
        SELECT 
            MIN(YEAR(date)) as min_year,
            MAX(YEAR(date)) as max_year,
            COUNT(DISTINCT YEAR(date)) as total_years
        FROM legal_acts 
        WHERE date IS NOT NULL
        """
        
        df = execute_query(years_query)
        
        if df.empty:
            return jsonify({'error': 'No date data found'})
        
        result = df.iloc[0].to_dict()
        
        # Get year distribution for smart filtering
        year_dist_query = """
        SELECT 
            YEAR(date) as year,
            COUNT(*) as count
        FROM legal_acts 
        WHERE date IS NOT NULL
        GROUP BY YEAR(date)
        ORDER BY year
        """
        
        year_dist_df = execute_query(year_dist_query)
        year_distribution = year_dist_df.to_dict('records') if not year_dist_df.empty else []
        
        # Create decade groupings for easier selection
        decades = {}
        for record in year_distribution:
            decade = (record['year'] // 10) * 10
            decade_label = f"{decade}s"
            if decade_label not in decades:
                decades[decade_label] = {
                    'start_year': decade,
                    'end_year': decade + 9,
                    'count': 0
                }
            decades[decade_label]['count'] += record['count']
        
        return jsonify({
            'min_year': int(result['min_year']),
            'max_year': int(result['max_year']),
            'total_years': int(result['total_years']),
            'year_distribution': year_distribution,
            'decades': list(decades.values())
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/analysis/individual-network', methods=['POST'])
def individual_network_analysis():
    """Generate network analysis centered on a specific individual - UPDATED with goods support"""
    try:
        data = request.get_json()
        
        individual_id = data.get('individual_id')
        good_id = data.get('good_id')  # NEW PARAMETER
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        measures = data.get('measures', [])
        analysis_type = data.get('analysis_type', 'ego_network')
        min_connections = int(data.get('min_connections', 1))
        
        if not individual_id:
            return jsonify({'error': 'Individual ID is required'})
        
        print(f"Building individual-centered network for {individual_id}, good_id={good_id}")
        print(f"Minimum connections filter: {min_connections}")
        
        # Build the individual's network with goods filtering
        if analysis_type == 'ego_network':
            G = build_individual_ego_network(individual_id, start_date, end_date, good_id)
        else:
            G = build_individual_direct_network(individual_id, start_date, end_date, good_id)
        
        if G.number_of_nodes() == 0:
            error_msg = f'No network connections found for individual {individual_id}'
            if good_id:
                error_msg += f' involving good "{good_id}"'
            return jsonify({'error': error_msg})
        
        # Apply filtering for individual networks
        if min_connections > 1:
            print(f"Applying filter to individual network (preserving focal individual)")
            
            focal_connections = G.degree(individual_id) if individual_id in G.nodes() else 0
            
            nodes_to_remove = []
            for node in G.nodes():
                if node != individual_id and G.degree(node) < min_connections:
                    nodes_to_remove.append(node)
            
            G.remove_nodes_from(nodes_to_remove)
            print(f"Removed {len(nodes_to_remove)} neighbors with < {min_connections} connections")
        
        if G.number_of_nodes() == 0:
            return jsonify({'error': f'No network remains after filtering for {min_connections}+ connections'})
        
        # Calculate metrics
        results = calculate_network_metrics(G, measures)
        
        # Add individual-specific information
        results['focal_individual'] = {
            'id': individual_id,
            'name': G.nodes.get(individual_id, {}).get('label', individual_id),
            'degree': G.degree(individual_id) if individual_id in G.nodes() else 0,
            'direct_connections': len(list(G.neighbors(individual_id))) if individual_id in G.nodes() else 0,
            'good_filter': good_id if good_id else None,
            'min_connections_filter': min_connections
        }
        
        return jsonify(results)
        
    except Exception as e:
        print(f"Individual network analysis error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Individual network analysis failed: {str(e)}'}), 500

# FIXED SECTIONS TO REPLACE IN YOUR APP.PY

# 1. Fix the network visualization function (around line 1800)
@app.route('/api/analysis/network-viz', methods=['POST'])
def network_visualization():
    """Generate network visualization data using Plotly - UPDATED with goods support"""
    try:
        data = request.get_json()
        
        # Extract parameters including good_id
        network_type = data.get('network_type', 'global')
        individual_id = data.get('individual_id')
        good_id = data.get('good_id')  # NEW PARAMETER
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        min_connections = int(data.get('min_connections', 1))
        layout_type = data.get('layout', 'spring')
        color_by = data.get('color_by', 'degree')
        size_by = data.get('size_by', 'degree')
        
        # Publication options
        show_labels = data.get('show_labels', True)
        label_color = data.get('label_color', 'white')
        black_white = data.get('black_white', False)
        
        print(f"Creating {network_type} network visualization, good_id={good_id}")
        
        # Build the network based on type
        if network_type == 'individual_centered' and individual_id:
            G = build_individual_ego_network(individual_id, start_date, end_date, good_id)
        elif network_type == 'goods_based' and good_id:
            G = build_network_from_db('global', start_date, end_date, None, good_id)
        else:
            G = build_network_from_db('global', start_date, end_date, None, good_id)
        
        if G.number_of_nodes() == 0:
            error_msg = 'No network data found for visualization'
            if good_id:
                error_msg += f' involving good "{good_id}"'
            return jsonify({'error': error_msg})
        
        # Apply minimum connections filter
        G_filtered = filter_network_by_connections(G, min_connections)
        
        if G_filtered.number_of_nodes() == 0:
            return jsonify({'error': f'No nodes have {min_connections} or more connections'})
        
        # Generate network layout
        pos = get_network_layout(G_filtered, layout_type)
        
        # Calculate node attributes for visualization
        node_attributes = calculate_node_attributes(G_filtered, color_by, size_by)
        
        # Create Plotly visualization with publication options
        fig = create_plotly_network(G_filtered, pos, node_attributes, network_type, individual_id,
                                   show_labels, label_color, black_white)
        
        # Convert to JSON for frontend
        graph_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        
        # Network statistics for display
        stats = {
            'num_nodes': G_filtered.number_of_nodes(),
            'num_edges': G_filtered.number_of_edges(),
            'density': float(nx.density(G_filtered)),
            'is_connected': nx.is_connected(G_filtered),
            'components': len(list(nx.connected_components(G_filtered)))
        }
        
        return jsonify({
            'graph_json': graph_json,
            'statistics': stats,
            'layout_used': layout_type,
            'good_filter': good_id,
            'nodes_filtered': G.number_of_nodes() - G_filtered.number_of_nodes()
        })
        
    except Exception as e:
        print(f"Network visualization error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Visualization failed: {str(e)}'}), 500

# Network stats full export function is defined later in the file (comprehensive version)

# 3. Fix the create_plotly_network function (remove duplicated parameters)
def create_plotly_network(G, pos, node_attributes, network_type='global', focal_individual=None):
    """Create Plotly network visualization - SIMPLIFIED VERSION"""
    
    # Create edge traces
    edge_x = []
    edge_y = []
    
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    )
    
    # Create node traces
    node_x = []
    node_y = []
    node_text = []
    node_colors = []
    node_sizes = []
    hover_text = []
    
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        
        attrs = node_attributes[node]
        
        # Node text (labels)
        node_text.append(attrs['label'])
        
        # Node colors
        node_colors.append(attrs['color_value'])
        
        # Node sizes (scale appropriately)
        base_size = 10
        size_multiplier = 30
        node_sizes.append(base_size + attrs['size_value'] * size_multiplier)
        
        # Hover information
        hover_info = f"<b>{attrs['label']}</b><br>"
        hover_info += f"Type: {attrs['type']}<br>"
        hover_info += f"Connections: {attrs['degree']}<br>"
        hover_info += f"ID: {node}"
        
        # Add focal individual indicator
        if focal_individual and node == focal_individual:
            hover_info += "<br><b>📍 FOCAL INDIVIDUAL</b>"
        
        hover_text.append(hover_info)
    
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        hoverinfo='text',
        hovertext=hover_text,
        text=node_text,
        textposition="middle center",
        textfont=dict(size=8, color='white'),
        marker=dict(
            showscale=True,
            colorscale='Viridis',
            reversescale=True,
            color=node_colors,
            size=node_sizes,
            colorbar=dict(
                thickness=15,
                len=0.7,
                x=1.02,
                title="Node Value",
                titleside="right"
            ),
            line=dict(width=2, color='white')
        )
    )
    
    # Create figure
    fig = go.Figure(data=[edge_trace, node_trace])
    
    # Update layout
    title = f"{network_type.replace('_', ' ').title()} Network Visualization"
    if focal_individual:
        focal_name = node_attributes.get(focal_individual, {}).get('label', focal_individual)
        title += f" - Centered on {focal_name}"
    
    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            font=dict(size=16)
        ),
        titlefont_size=16,
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20,l=5,r=5,t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='white',
        height=600
    )
    
    return fig

@app.route('/api/analysis/network-stats-full-export', methods=['POST'])
def network_stats_full_export():
    """Full export of network statistics without limits"""
    try:
        data = request.get_json()
        
        network_type = data.get('network_type', 'global')
        individual_id = data.get('individual_id')
        good_id = data.get('good_id')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        min_connections = int(data.get('min_connections', 1))
        measures = data.get('measures', [])
        export_format = data.get('export_format', 'csv')
        
        # Build network with goods filtering
        if network_type == 'individual_centered' and individual_id:
            G = build_individual_ego_network(individual_id, start_date, end_date, good_id)
        else:
            G = build_network_from_db('global', start_date, end_date, None, good_id)
        
        if G.number_of_nodes() == 0:
            return jsonify({'error': 'No network data to export'})
        
        # Apply filters
        G_filtered = filter_network_by_connections(G, min_connections)
        
        # Calculate ALL metrics for ALL nodes
        results = calculate_network_metrics(G_filtered, measures)
        
        if export_format == 'csv':
            import tempfile
            import csv
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp_file:
                writer = csv.writer(tmp_file)
                
                # Write header
                header = ['Node_ID', 'Node_Name', 'Node_Type', 'Degree']
                if 'degree' in measures and 'degree_centrality' in results and not results['degree_centrality'].get('error'):
                    header.append('Degree_Centrality')
                if 'betweenness' in measures and 'betweenness_centrality' in results and not results['betweenness_centrality'].get('error'):
                    header.append('Betweenness_Centrality')
                if 'closeness' in measures and 'closeness_centrality' in results and not results['closeness_centrality'].get('error'):
                    header.append('Closeness_Centrality')
                if 'eigenvector' in measures and 'eigenvector_centrality' in results and not results['eigenvector_centrality'].get('error'):
                    header.append('Eigenvector_Centrality')
                
                writer.writerow(header)
                
                # Write data for each node
                for node in G_filtered.nodes():
                    row = [
                        node,
                        G_filtered.nodes[node].get('label', node),
                        G_filtered.nodes[node].get('type', 'unknown'),
                        G_filtered.degree(node)
                    ]
                    
                    # Add centrality measures if available
                    if 'degree' in measures and 'degree_centrality' in results and not results['degree_centrality'].get('error'):
                        degree_cent = results['degree_centrality'].get('top_nodes', {})
                        node_name = G_filtered.nodes[node].get('label', node)
                        row.append(degree_cent.get(node_name, 0))
                    
                    if 'betweenness' in measures and 'betweenness_centrality' in results and not results['betweenness_centrality'].get('error'):
                        between_cent = results['betweenness_centrality'].get('top_nodes', {})
                        node_name = G_filtered.nodes[node].get('label', node)
                        row.append(between_cent.get(node_name, 0))
                    
                    if 'closeness' in measures and 'closeness_centrality' in results and not results['closeness_centrality'].get('error'):
                        close_cent = results['closeness_centrality'].get('top_nodes', {})
                        node_name = G_filtered.nodes[node].get('label', node)
                        row.append(close_cent.get(node_name, 0))
                    
                    if 'eigenvector' in measures and 'eigenvector_centrality' in results and not results['eigenvector_centrality'].get('error'):
                        eigen_cent = results['eigenvector_centrality'].get('top_nodes', {})
                        node_name = G_filtered.nodes[node].get('label', node)
                        row.append(eigen_cent.get(node_name, 0))
                    
                    writer.writerow(row)
                
                filename = f"network_analysis_full_{network_type}"
                if good_id:
                    filename += f"_good_{good_id}"
                if individual_id:
                    filename += f"_individual_{individual_id}"
                filename += ".csv"
                
                return send_file(tmp_file.name, as_attachment=True, 
                               download_name=filename, mimetype='text/csv')
        
        elif export_format == 'pdf':
            # For PDF, create a comprehensive report
            from reportlab.lib.pagesizes import letter, A4
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import inch
            from reportlab.lib import colors
            import tempfile
            from datetime import datetime
            
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                doc = SimpleDocTemplate(tmp_file.name, pagesize=A4)
                styles = getSampleStyleSheet()
                story = []
                
                # Title
                title_style = ParagraphStyle(
                    'CustomTitle',
                    parent=styles['Heading1'],
                    fontSize=18,
                    spaceAfter=30,
                    alignment=1  # Center
                )
                story.append(Paragraph("Complete Network Analysis Report", title_style))
                story.append(Spacer(1, 12))
                
                # Report metadata
                meta_style = styles['Normal']
                story.append(Paragraph(f"<b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", meta_style))
                story.append(Paragraph(f"<b>Network Type:</b> {network_type.replace('_', ' ').title()}", meta_style))
                if individual_id:
                    story.append(Paragraph(f"<b>Individual ID:</b> {individual_id}", meta_style))
                if good_id:
                    story.append(Paragraph(f"<b>Good ID:</b> {good_id}", meta_style))
                if start_date and end_date:
                    story.append(Paragraph(f"<b>Date Range:</b> {start_date} to {end_date}", meta_style))
                story.append(Paragraph(f"<b>Minimum Connections:</b> {min_connections}", meta_style))
                story.append(Spacer(1, 20))
                
                # Network Overview
                story.append(Paragraph("Network Overview", styles['Heading2']))
                story.append(Paragraph(f"Total Nodes: {G_filtered.number_of_nodes()}", meta_style))
                story.append(Paragraph(f"Total Edges: {G_filtered.number_of_edges()}", meta_style))
                if G_filtered.number_of_nodes() > 0:
                    density = nx.density(G_filtered)
                    story.append(Paragraph(f"Network Density: {density:.4f}", meta_style))
                story.append(Spacer(1, 20))
                
                # Detailed Node Data Table
                story.append(Paragraph("Complete Node Analysis", styles['Heading2']))
                
                # Prepare table data
                table_data = [['Node ID', 'Node Name', 'Type', 'Degree']]
                
                # Add centrality columns if calculated
                if 'degree' in measures and 'degree_centrality' in results and not results['degree_centrality'].get('error'):
                    table_data[0].append('Degree Centrality')
                if 'betweenness' in measures and 'betweenness_centrality' in results and not results['betweenness_centrality'].get('error'):
                    table_data[0].append('Betweenness Centrality')
                if 'closeness' in measures and 'closeness_centrality' in results and not results['closeness_centrality'].get('error'):
                    table_data[0].append('Closeness Centrality')
                if 'eigenvector' in measures and 'eigenvector_centrality' in results and not results['eigenvector_centrality'].get('error'):
                    table_data[0].append('Eigenvector Centrality')
                
                # Add all node data
                for node in G_filtered.nodes():
                    row = [
                        str(node),
                        str(G_filtered.nodes[node].get('label', node)),
                        str(G_filtered.nodes[node].get('type', 'unknown')),
                        str(G_filtered.degree(node))
                    ]
                    
                    # Add centrality measures if available
                    if 'degree' in measures and 'degree_centrality' in results and not results['degree_centrality'].get('error'):
                        degree_cent = results['degree_centrality'].get('top_nodes', {})
                        node_name = G_filtered.nodes[node].get('label', node)
                        row.append(f"{degree_cent.get(node_name, 0):.4f}")
                    
                    if 'betweenness' in measures and 'betweenness_centrality' in results and not results['betweenness_centrality'].get('error'):
                        between_cent = results['betweenness_centrality'].get('top_nodes', {})
                        node_name = G_filtered.nodes[node].get('label', node)
                        row.append(f"{between_cent.get(node_name, 0):.4f}")
                    
                    if 'closeness' in measures and 'closeness_centrality' in results and not results['closeness_centrality'].get('error'):
                        close_cent = results['closeness_centrality'].get('top_nodes', {})
                        node_name = G_filtered.nodes[node].get('label', node)
                        row.append(f"{close_cent.get(node_name, 0):.4f}")
                    
                    if 'eigenvector' in measures and 'eigenvector_centrality' in results and not results['eigenvector_centrality'].get('error'):
                        eigen_cent = results['eigenvector_centrality'].get('top_nodes', {})
                        node_name = G_filtered.nodes[node].get('label', node)
                        row.append(f"{eigen_cent.get(node_name, 0):.4f}")
                    
                    table_data.append(row)
                
                # Create and style the table
                table = Table(table_data)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                
                story.append(table)
                
                # Build PDF
                doc.build(story)
                
                filename = f"network_analysis_complete_{network_type}"
                if good_id:
                    filename += f"_good_{good_id}"
                if individual_id:
                    filename += f"_individual_{individual_id}"
                filename += ".pdf"
                
                return send_file(tmp_file.name, as_attachment=True, 
                               download_name=filename, mimetype='application/pdf')
        
        else:
            return jsonify({'error': 'Unsupported export format'})
        
    except Exception as e:
        print(f"Full export error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Export failed: {str(e)}'}), 500

@app.route('/api/debug/goods-available')
def debug_goods_available():
    """Debug what goods are actually available in the database"""
    try:
        # Check what goods exist in goods table (since good_price is empty)
        goods_query = """
        SELECT 
            g.good_id,
            g.Name as good_name,
            g.Description
        FROM goods g
        ORDER BY g.Name
        LIMIT 50
        """
        
        df = execute_query(goods_query)
        
        goods_summary = {
            'total_unique_goods': len(df) if not df.empty else 0,
            'goods_list': df.to_dict('records') if not df.empty else [],
            'note': 'Searching goods table directly since good_price table is empty'
        }
        
        # Check good_price table status
        price_check_query = "SELECT COUNT(*) as count FROM good_price"
        price_df = execute_query(price_check_query)
        goods_summary['good_price_records'] = price_df.iloc[0]['count'] if not price_df.empty else 0
        
        # Check if there are any legal acts with goods relationships
        la_goods_query = """
        SELECT COUNT(*) as count
        FROM la_gp lgp
        INNER JOIN good_price gp ON lgp.gp_id = gp.gp_id
        """
        
        la_goods_df = execute_query(la_goods_query)
        goods_summary['legal_acts_with_goods'] = la_goods_df.iloc[0]['count'] if not la_goods_df.empty else 0
        
        return jsonify(goods_summary)
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/search/goods')
def search_goods():
    """Fixed goods search endpoint - searches goods table directly"""
    try:
        search_term = request.args.get('q', '').strip()
        
        if len(search_term) < 2:
            return jsonify({'goods': []})
        
        # Search goods table directly since good_price table is empty
        search_query = """
        SELECT 
            g.good_id, 
            g.Name as good_name,
            g.Description
        FROM goods g
        WHERE (g.Name LIKE %s OR g.Description LIKE %s OR g.good_id LIKE %s)
        ORDER BY g.Name
        LIMIT 20
        """
        
        search_pattern = f"%{search_term}%"
        df = execute_query(search_query, params=[search_pattern, search_pattern, search_pattern])
        
        goods = []
        if not df.empty:
            for _, row in df.iterrows():
                good_name = str(row['good_name']) if pd.notna(row['good_name']) else str(row['good_id'])
                goods.append({
                    'id': row['good_id'],
                    'name': good_name,
                    'description': str(row['Description']) if pd.notna(row['Description']) else ''
                })
        
        return jsonify({'goods': goods})
        
    except Exception as e:
        print(f"Goods search error: {e}")
        return jsonify({'error': str(e)})

# 2. UPDATE the create_plotly_network function with new parameters
def create_plotly_network(G, pos, node_attributes, network_type='global', focal_individual=None,
                         show_labels=True, label_color='white', black_white=False):
    """Create Plotly network visualization - ENHANCED with publication options"""
    
    # Create edge traces
    edge_x = []
    edge_y = []
    edge_info = []
    
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        
        # Edge information for hover
        weight = G[edge[0]][edge[1]].get('weight', 1)
        source_label = node_attributes[edge[0]]['label']
        target_label = node_attributes[edge[1]]['label']
        edge_info.append(f"{source_label} ↔ {target_label} (Weight: {weight})")
    
    # Edge color based on black_white setting
    edge_color = '#000000' if black_white else '#888888'
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color=edge_color),
        hoverinfo='none',
        mode='lines'
    )
    
    # Create node traces
    node_x = []
    node_y = []
    node_text = []
    node_colors = []
    node_sizes = []
    hover_text = []
    
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        
        attrs = node_attributes[node]
        
        # Node text (labels) - show or hide based on setting
        if show_labels:
            node_text.append(attrs['label'])
        else:
            node_text.append('')  # Empty labels
        
        # Node colors - handle black/white mode
        if black_white:
            # In black/white mode, convert color values to grayscale (0-1 range)
            # Normalize the color value to grayscale
            normalized_value = attrs['color_value']
            if isinstance(normalized_value, (int, float)):
                # Ensure value is between 0 and 1 for grayscale
                gray_value = max(0, min(1, normalized_value))
            else:
                gray_value = 0.5  # Default gray
            node_colors.append(gray_value)
        else:
            # Normal color mode
            node_colors.append(attrs['color_value'])
        
        # Node sizes (scale appropriately)
        base_size = 10
        size_multiplier = 30
        node_sizes.append(base_size + attrs['size_value'] * size_multiplier)
        
        # Hover information
        hover_info = f"<b>{attrs['label']}</b><br>"
        hover_info += f"Type: {attrs['type']}<br>"
        hover_info += f"Connections: {attrs['degree']}<br>"
        hover_info += f"ID: {node}"
        
        # Add focal individual indicator
        if focal_individual and node == focal_individual:
            hover_info += "<br><b>📍 FOCAL INDIVIDUAL</b>"
        
        hover_text.append(hover_info)
    
    # Determine color scale based on black/white setting
    if black_white:
        color_scale = 'Greys'  # Grayscale for black/white mode
        colorbar_title = "Node Value (Grayscale)"
    else:
        color_scale = 'Viridis'  # Default colorful mode
        colorbar_title = "Node Value"
    
    # Create node trace
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text' if show_labels else 'markers',  # Show text only if labels enabled
        hoverinfo='text',
        hovertext=hover_text,
        text=node_text,
        textposition="middle center",
        textfont=dict(
            size=8, 
            color=label_color if show_labels else 'rgba(0,0,0,0)'  # Hide text color if no labels
        ),
        marker=dict(
            showscale=True,
            colorscale=color_scale,
            reversescale=True,
            color=node_colors,
            size=node_sizes,
            colorbar=dict(
                thickness=15,
                len=0.7,
                x=1.02,
                title=colorbar_title,
                titleside="right"
            ),
            line=dict(
                width=2, 
                color='black' if black_white else 'white'  # Border color
            )
        )
    )
    
    # Highlight focal individual if present
    if focal_individual and focal_individual in pos:
        focal_x, focal_y = pos[focal_individual]
        focal_color = 'black' if black_white else 'red'
        
        focal_trace = go.Scatter(
            x=[focal_x], y=[focal_y],
            mode='markers',
            marker=dict(
                size=max(node_sizes) + 15,
                color=focal_color,
                symbol='circle-open',
                line=dict(width=4, color=focal_color)
            ),
            hoverinfo='skip',
            showlegend=False,
            name='Focal Individual'
        )
        
        # Create figure with focal highlight
        fig = go.Figure(data=[edge_trace, node_trace, focal_trace])
    else:
        fig = go.Figure(data=[edge_trace, node_trace])
    
    # Update layout
    title = f"{network_type.replace('_', ' ').title()} Network Visualization"
    if focal_individual:
        focal_name = node_attributes.get(focal_individual, {}).get('label', focal_individual)
        title += f" - Centered on {focal_name}"
    
    # Background color based on black/white setting
    bg_color = 'white' if black_white else 'white'
    
    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            font=dict(size=16, color='black' if black_white else '#2c3e50')
        ),
        titlefont_size=16,
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20,l=5,r=5,t=40),
        annotations=[ dict(
            text="",
            showarrow=False,
            xref="paper", yref="paper",
            x=0.005, y=-0.002,
            xanchor='left', yanchor='bottom',
            font=dict(color='gray', size=12)
        )],
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor=bg_color,
        paper_bgcolor=bg_color,
        height=600
    )
    
    return fig


@app.route('/api/analysis/network-export-academic', methods=['POST'])
@beta_required
@exports_required
def export_academic_figure():
    """Export high-quality figures optimized for academic publications"""
    try:
        data = request.get_json()
        
        # Extract all visualization parameters
        network_type = data.get('network_type', 'global')
        individual_id = data.get('individual_id')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        min_connections = int(data.get('min_connections', 1))
        layout_type = data.get('layout', 'spring')
        color_by = data.get('color_by', 'degree')
        size_by = data.get('size_by', 'degree')
        
        # Publication options
        show_labels = data.get('show_labels', False)  # Default no labels for publication
        label_color = data.get('label_color', 'black')
        black_white = data.get('black_white', True)  # Default B&W for publication
        
        # Image options
        width = int(data.get('width', 1200))
        height = int(data.get('height', 900))
        dpi = int(data.get('dpi', 300))  # High DPI for publication
        
        # Build network (same as regular visualization)
        if network_type == 'individual_centered' and individual_id:
            G = build_individual_ego_network(individual_id, start_date, end_date)
        else:
            G = build_network_from_db('global', start_date, end_date)
        
        if G.number_of_nodes() == 0:
            return jsonify({'error': 'No network data found'})
        
        G_filtered = filter_network_by_connections(G, min_connections)
        
        if G_filtered.number_of_nodes() == 0:
            return jsonify({'error': 'No nodes meet criteria'})
        
        # Generate layout and create figure
        pos = get_network_layout(G_filtered, layout_type)
        node_attributes = calculate_node_attributes(G_filtered, color_by, size_by)
        fig = create_plotly_network(G_filtered, pos, node_attributes, network_type, individual_id,
                                  show_labels, label_color, black_white)
        
        # Export as high-quality image
        import tempfile
        import io
        
        # Create temporary file for the image
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
            # Export with high quality settings
            fig.write_image(
                tmp_file.name,
                format='png',
                width=width,
                height=height,
                scale=dpi/72  # Convert DPI to scale factor
            )
            
            # Generate filename based on parameters
            filename_parts = [network_type]
            if individual_id:
                filename_parts.append(f"individual_{individual_id}")
            if start_date and end_date:
                start_year = start_date.split('-')[0]
                end_year = end_date.split('-')[0]
                filename_parts.append(f"{start_year}-{end_year}")
            if black_white:
                filename_parts.append("bw")
            if not show_labels:
                filename_parts.append("no_labels")
            
            filename = "_".join(filename_parts) + "_academic.png"
            
            return send_file(
                tmp_file.name, 
                as_attachment=True, 
                download_name=filename,
                mimetype='image/png'
            )
        
    except Exception as e:
        print(f"Academic export error: {e}")
        return jsonify({'error': f'Export failed: {str(e)}'}), 500

@app.route('/api/analysis/economic-stats', methods=['POST'])
def economic_statistics():
    """Generate economic statistics and econometric analysis"""
    try:
        data = request.get_json()
        
        # Extract parameters
        goods = data.get('goods', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        analyses = data.get('analyses', [])
        
        print(f"Economic analysis: goods={goods}, analyses={analyses}")
        
        # Get price data from database
        price_data = get_price_data_from_db(goods, start_date, end_date)
        
        if price_data.empty:
            return jsonify({'error': 'No price data found for specified criteria'})
        
        results = {}
        
        # Descriptive Statistics
        if 'descriptive' in analyses:
            results['descriptive_stats'] = calculate_descriptive_stats(price_data)
        
        if 'volatility' in analyses:
            results['volatility_analysis'] = calculate_volatility(price_data)
        
        # Add data summary
        results['data_summary'] = {
            'total_observations': len(price_data),
            'goods_analyzed': price_data['good_name'].nunique(),
            'date_range': {
                'start': str(price_data['date'].min()),
                'end': str(price_data['date'].max())
            },
            'currencies': price_data['currency'].unique().tolist()
        }
        
        return jsonify(results)
        
    except Exception as e:
        print(f"Economic analysis error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analysis/economic-viz', methods=['POST'])
def economic_visualization():
    """Generate economic visualization data"""
    try:
        data = request.get_json()
        
        chart_type = data.get('chart_type', 'line')
        goods = data.get('goods', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # Get price data
        price_data = get_price_data_from_db(goods, start_date, end_date)
        
        if price_data.empty:
            return jsonify({'error': 'No data found for visualization'})
        
        # Convert date column to datetime
        price_data['date'] = pd.to_datetime(price_data['date'])
        
        # Generate chart data based on type
        if chart_type == 'line':
            chart_data = {
                'type': 'line',
                'data': {},
                'layout': {
                    'title': 'Price Trends Over Time',
                    'xaxis': {'title': 'Date'},
                    'yaxis': {'title': 'Price'}
                }
            }
            
            for good in price_data['good_name'].unique():
                good_data = price_data[price_data['good_name'] == good].sort_values('date')
                chart_data['data'][good] = {
                    'x': good_data['date'].dt.strftime('%Y-%m-%d').tolist(),
                    'y': good_data['price'].tolist(),
                    'type': 'scatter',
                    'mode': 'lines+markers',
                    'name': good
                }
        
        else:
            # Default to summary statistics
            chart_data = {
                'type': 'summary',
                'data': calculate_descriptive_stats(price_data)
            }
        
        return jsonify(chart_data)
        
    except Exception as e:
        print(f"Economic visualization error: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# DEBUG ENDPOINTS
# ============================================================================

@app.route('/api/debug/network-data-detailed')
def debug_network_data_detailed():
    """Comprehensive debug endpoint to understand your database structure"""
    try:
        debug_info = {}
        
        # 1. Check table existence and row counts
        tables_to_check = ['imt', 'legal_acts', 'la_party_1', 'la_party_2', 'la_party_1_o', 'la_party_2_o', 'la_mentions']
        for table in tables_to_check:
            try:
                count_query = f"SELECT COUNT(*) as count FROM {table}"
                result = execute_query(count_query)
                debug_info[f"{table}_count"] = result.iloc[0]['count'] if not result.empty else 0
            except Exception as e:
                debug_info[f"{table}_error"] = str(e)
        
        # 2. Sample data from key tables
        try:
            sample_imt = execute_query("SELECT i_id, FiName, LaName1 FROM imt LIMIT 5")
            debug_info['sample_individuals'] = sample_imt.to_dict('records') if not sample_imt.empty else []
        except Exception as e:
            debug_info['sample_individuals_error'] = str(e)
        
        try:
            sample_legal_acts = execute_query("SELECT la_id, date, type FROM legal_acts LIMIT 5")
            debug_info['sample_legal_acts'] = sample_legal_acts.to_dict('records') if not sample_legal_acts.empty else []
        except Exception as e:
            debug_info['sample_legal_acts_error'] = str(e)
        
        # 3. Check for actual connections
        try:
            connections_query = """
            SELECT 
                p1.i_id as person1,
                p2.i_id as person2,
                la.date,
                la.la_id
            FROM la_party_1 p1
            JOIN la_party_2 p2 ON p1.la_id = p2.la_id
            JOIN legal_acts la ON p1.la_id = la.la_id
            WHERE p1.i_id IS NOT NULL AND p2.i_id IS NOT NULL
            LIMIT 10
            """
            connections_result = execute_query(connections_query)
            debug_info['sample_connections'] = connections_result.to_dict('records') if not connections_result.empty else []
        except Exception as e:
            debug_info['sample_connections_error'] = str(e)
        
        # 4. Check date formats
        try:
            date_query = "SELECT DISTINCT date FROM legal_acts ORDER BY date LIMIT 10"
            date_result = execute_query(date_query)
            debug_info['sample_dates'] = date_result.to_dict('records') if not date_result.empty else []
        except Exception as e:
            debug_info['date_format_error'] = str(e)
        
        # 5. Check for same-side connections (partnerships)
        try:
            partnerships_query = """
            SELECT 
                p1.i_id as partner1,
                p2.i_id as partner2,
                COUNT(*) as frequency
            FROM la_party_1 p1
            JOIN la_party_1 p2 ON p1.la_id = p2.la_id AND p1.i_id < p2.i_id
            WHERE p1.i_id IS NOT NULL AND p2.i_id IS NOT NULL
            GROUP BY p1.i_id, p2.i_id
            LIMIT 5
            """
            partnerships_result = execute_query(partnerships_query)
            debug_info['sample_partnerships'] = partnerships_result.to_dict('records') if not partnerships_result.empty else []
        except Exception as e:
            debug_info['partnerships_error'] = str(e)
        
        return jsonify(debug_info)
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/debug/test-network-build')
def test_network_build():
    """Test the network building process step by step"""
    try:
        # Test building a small network
        G = build_network_from_db('global')
        
        result = {
            'network_built': True,
            'num_nodes': G.number_of_nodes(),
            'num_edges': G.number_of_edges(),
            'sample_nodes': list(G.nodes())[:10],
            'sample_edges': list(G.edges())[:10],
            'node_attributes_sample': {}
        }
        
        # Get sample node attributes
        for node in list(G.nodes())[:3]:
            result['node_attributes_sample'][node] = dict(G.nodes[node])
        
        return jsonify(result)
        
    except Exception as e:
        import traceback
        return jsonify({
            'network_built': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        })

@app.route('/api/debug/test-simple-query')
def test_simple_query():
    """Test a simple query to verify database connection"""
    try:
        # Simple test query
        test_query = "SELECT COUNT(*) as total FROM imt"
        result = execute_query(test_query)
        
        if not result.empty:
            return jsonify({
                'database_connected': True,
                'total_individuals': int(result.iloc[0]['total']),
                'query_successful': True
            })
        else:
            return jsonify({
                'database_connected': False,
                'error': 'Query returned empty result'
            })
    
    except Exception as e:
        return jsonify({
            'database_connected': False,
            'error': str(e)
        })

@app.route('/api/debug/network-data')
def debug_network_data():
    """Debug endpoint to check network data availability"""
    try:
        # Check individuals business connections
        individuals_query = """
        SELECT COUNT(*) as count FROM la_party_1 p1
        JOIN la_party_2 p2 ON p1.la_id = p2.la_id
        WHERE p1.i_id != p2.i_id
        AND p1.i_id IS NOT NULL AND p2.i_id IS NOT NULL
        """
        individuals_result = execute_query(individuals_query)
        
        # Check organizations
        orgs_query = """
        SELECT COUNT(*) as count FROM la_party_1_o p1
        JOIN la_party_2_o p2 ON p1.la_id = p2.la_id
        WHERE p1.o_id != p2.o_id
        AND p1.o_id IS NOT NULL AND p2.o_id IS NOT NULL
        """
        orgs_result = execute_query(orgs_query)
        
        # Sample connections
        sample_query = """
        SELECT 
            p1.i_id as source,
            p2.i_id as target,
            la.date
        FROM la_party_1 p1
        JOIN la_party_2 p2 ON p1.la_id = p2.la_id
        JOIN legal_acts la ON p1.la_id = la.la_id
        WHERE p1.i_id != p2.i_id
        AND p1.i_id IS NOT NULL AND p2.i_id IS NOT NULL
        LIMIT 10
        """
        sample_result = execute_query(sample_query)
        
        return jsonify({
            'individual_connections': individuals_result.iloc[0]['count'] if not individuals_result.empty else 0,
            'organization_connections': orgs_result.iloc[0]['count'] if not orgs_result.empty else 0,
            'sample_connections': sample_result.to_dict('records') if not sample_result.empty else []
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/debug/price-data')
def debug_price_data():
    """Debug endpoint to check price data availability"""
    try:
        # Check available goods
        goods_query = """
        SELECT 
            gp.good, 
            COUNT(*) as price_observations,
            MIN(gp.Rate) as min_price,
            MAX(gp.Rate) as max_price,
            AVG(gp.Rate) as avg_price
        FROM good_price gp 
        WHERE gp.Rate IS NOT NULL AND gp.Rate > 0
        GROUP BY gp.good 
        ORDER BY COUNT(*) DESC
        """
        goods_result = execute_query(goods_query)
        
        return jsonify({
            'available_goods': goods_result.to_dict('records') if not goods_result.empty else []
        })
        
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    # Get port from environment variable (Railway sets this)
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') == 'development'
    
    print("=" * 60)
    print("🚀 Starting Maonas Database Application")
    print("=" * 60)
    print(f"🌐 Port: {port}")
    print(f"🔧 Debug: {debug}")
    print(f"🗃️ Database: {'PostgreSQL (Railway)' if os.environ.get('DATABASE_URL') else 'MySQL (Local)'}")
    print("📊 Features: Social Network Analysis + Economic Tools")
    print("🔒 Beta Protection: Enabled")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=port, debug=debug)
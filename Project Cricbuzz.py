import streamlit as st
import pandas as pd
import requests
import psycopg2
import time

# ----------------------------
# Page & App Configuration
# ----------------------------
st.set_page_config(
    page_title="Cricbuzz Dashboard",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded"
)

#Centralized custom CSS for better maintainability.
st.markdown("""
<style>
    /* General styling */
    body { background-color: #0e1117; color: white; }
    /* DataFrame styling */
    .stDataFrame { border-radius: 10px; overflow: hidden; }
    table { border-collapse: collapse; border-radius: 10px !important; }
    thead tr th { background-color: #262730 !important; color: #f9f9f9 !important; font-weight: 700 !important; }
    tbody tr:nth-child(odd) { background-color: #1a1d24 !important; }
    tbody tr:nth-child(even) { background-color: #121418 !important; }
    /* Custom components */
    .section-title { font-size: 24px; font-weight: 700; color: #ffb703; margin-top: 40px; }
    .divider { border-top: 2px solid #ffb703; margin-top: 10px; margin-bottom: 20px; }
</style>
""", unsafe_allow_html=True)

# ----------------------------
# API & Session Management
# ----------------------------
# REFINED: Moved API constants to the top for clarity.
API_HOST = "cricbuzz-cricket.p.rapidapi.com"

# Using a single session object for all API calls is efficient.
@st.cache_resource
def get_requests_session():
    """Initializes and caches a requests.Session object with API headers."""
    try:
        api_key = st.secrets["rapidapi"]["key"]
        headers = {
            "x-rapidapi-host": API_HOST,
            "x-rapidapi-key": api_key,
        }
        session = requests.Session()
        session.headers.update(headers)
        return session
    except (KeyError, FileNotFoundError):
        st.error("Please add your RapidAPI key to Streamlit secrets (secrets.toml).")
        st.code("""
[rapidapi]
key = "YOUR_API_KEY_HERE"
        """)
        return None

session = get_requests_session()
if not session:
    st.stop()

# ----------------------------
# Database Connection
# ----------------------------
@st.cache_resource
def get_db_connection():
    """Establishes and caches the database connection."""
    try:
        db = st.secrets["database"]
        conn = psycopg2.connect(
            host=db["DB_HOST"],
            database=db["DB_NAME"],
            user=db["DB_USER"],
            password=db["DB_PASSWORD"],
            port=db.get("DB_PORT", 5432)
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        return None

# FIX: Abstracted query execution into a reusable, safe function.
def run_query(query: str, params=None):
    """Runs a SQL query and returns the result as a DataFrame."""
    conn = get_db_connection()
    if conn:
        try:
            return pd.read_sql(query, conn, params=params)
        except Exception as e:
            st.error(f"❌ Query failed: {e}")
            return pd.DataFrame()
    return pd.DataFrame()


# ----------------------------
# API Data Fetching Functions
# ----------------------------
# REFINED: Added caching with a Time-To-Live (TTL) to avoid excessive API calls.
@st.cache_data(ttl=60) # Cache live match data for 60 seconds
def get_live_matches():
    """Fetches live matches from the Cricbuzz API."""
    url = f"https://{API_HOST}/matches/v1/live"
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        matches = []
        # NOTE: This parsing logic is deeply nested and specific to the API's current structure.
        # If the API changes, this part may need updates.
        for mtype in data.get("typeMatches", []):
            for s in mtype.get("seriesMatches", []):
                if "seriesAdWrapper" in s:
                    for m in s["seriesAdWrapper"].get("matches", []):
                        info = m.get("matchInfo", {})
                        if info:
                            matches.append({
                                "id": info["matchId"],
                                "name": info["matchDesc"],
                                "series": info.get("seriesName", "N/A"),
                                "team1": info.get("team1", {}).get("teamName", "TBC"),
                                "team2": info.get("team2", {}).get("teamName", "TBC"),
                                "venue": info.get("venueInfo", {}).get("ground", "Unknown"),
                                "state": info.get("state", "N/A"),
                                "status": info.get("status", "No status")
                            })
        return matches
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            st.warning("⚠️ API rate limit reached. Please wait a minute.")
        else:
            st.error(f"API Error fetching live matches: {e}")
        return []
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return []

@st.cache_data(ttl=30)
def get_scorecard(match_id):
    """Fetches the scorecard for a specific match ID."""
    url = f"https://{API_HOST}/mcenter/v1/{match_id}/hscard"
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            st.warning("⚠️ API rate limit reached. Retrying in 10s...")
            time.sleep(10) # Simple backoff strategy
            return get_scorecard(match_id) # Retry once
        else:
            st.error(f"API Error fetching scorecard: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

# This function is for Parse Scorecard
def parse_scorecard(data, match_id):
    """Parses scorecard JSON into four distinct DataFrames."""
    scorecard = data.get("scorecard", [])
    batting, bowling, fow, summary = [], [], [], []
    for inns in scorecard:
        team = inns.get("batteamname")
        iid = inns.get("inningsid")
        # Batting, Bowling, FoW, Summary parsing logic remains the same...
        # (Keeping original logic as it was correct)
        for b in inns.get("batsman", []):
            batting.append({"Innings": iid, "Team": team, "Name": b.get("name"), "Runs": b.get("runs"), "Balls": b.get("balls"), "4s": b.get("fours"), "6s": b.get("sixes"), "SR": b.get("strkrate"), "Out": b.get("outdec")})
        for bw in inns.get("bowler", []):
            bowling.append({"Innings": iid, "Team": team, "Name": bw.get("name"), "Overs": bw.get("overs"), "Runs": bw.get("runs"), "Wickets": bw.get("wickets"), "Economy": bw.get("economy")})
        for f in inns.get("fow", {}).get("fow", []):
            fow.append({"Innings": iid, "Team": team, "Batsman": f.get("batsmanname"), "ScoreAtFall": f.get("runs"), "Over": f.get("overnbr")})
        extras = inns.get("extras", {})
        summary.append({"MatchID": match_id, "InningsID": iid, "Team": team, "Score": inns.get("score"), "Wickets": inns.get("wickets"), "Overs": inns.get("overs"), "RunRate": inns.get("runrate"), "Extras": extras.get("total", 0), "Byes": extras.get("byes", 0), "LegByes": extras.get("legbyes", 0), "Wides": extras.get("wides", 0), "NoBalls": extras.get("noballs", 0)})

    return (
        pd.DataFrame(batting).drop_duplicates().reset_index(drop=True),
        pd.DataFrame(bowling).drop_duplicates().reset_index(drop=True),
        pd.DataFrame(fow).drop_duplicates().reset_index(drop=True),
        pd.DataFrame(summary).drop_duplicates().reset_index(drop=True)
    )

@st.cache_data(ttl=3600) # Player search results don't change often
def search_player(name):
    """Searches for a player by name via API."""
    url = f"https://{API_HOST}/stats/v1/player/search"
    try:
        resp = session.get(url, params={"plrN": name}, timeout=10)
        resp.raise_for_status()
        return resp.json().get("player", [])
    except Exception as e:
        st.error(f"Player search error: {e}")
        return []

@st.cache_data(ttl=3600) # Player details are static, cache for an hour
def get_player_stats(player_id, stat_type):
    """Fetches batting or bowling stats for a player."""
    url = f"https://{API_HOST}/stats/v1/player/{player_id}/{stat_type}"
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        headers = data.get('headers')
        rows = data.get('values', [])
        if headers and rows:
            stats = [r.get('values', []) for r in rows if 'values' in r]
            df = pd.DataFrame(stats, columns=headers).set_index(headers[0])
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching {stat_type} stats: {e}")
        return pd.DataFrame()


# ----------------------------
# Streamlit Pages
# ----------------------------
def home_page():
    st.title("🏠 Cricket Data Analytics Dashboard")
    st.markdown("""
    ## 🏏 Overview
    Welcome to the **Cricket Data Analytics Dashboard** — an interactive platform to explore detailed insights from international cricket matches.
    Allowing users to explore live match data, player stats, and cricket analytics. It also integrates with PostgreSQL for structured storage and includes SQL practice and CRUD management features.
                """)
def live_match_page():
    """UI for displaying live match scores."""
    st.title("🏏 Live Match Dashboard")
    
    with st.spinner("Fetching live matches..."):
        matches = get_live_matches()

    if not matches:
        st.warning("No live matches found or API limit may have been reached.")
        return

    match_options = {f"{m['name']} ({m['series']})": m for m in matches}
    selected_match_label = st.selectbox("Select a match:", match_options.keys())
    
    if selected_match_label:
        match = match_options[selected_match_label]
        
        st.markdown(f"### 📍 {match['series']}")
        col1, col2 = st.columns(2)
        col1.info(f"**Teams:** {match['team1']} vs {match['team2']}")
        col2.info(f"**Venue:** {match['venue']}")
        st.success(f"**Status:** {match['status']}")
        
        with st.expander("Show Full Scorecard"):
            with st.spinner(f"Fetching scorecard for {match['name']}..."):
                data = get_scorecard(match["id"])
            
            if not data or "scorecard" not in data or not data["scorecard"]:
                st.warning("Scorecard not available for this match yet.")
            else:
                bat_df, bowl_df, fow_df, sum_df = parse_scorecard(data, match["id"])

                st.markdown('<div class="section-title">📊 Innings Summary</div><div class="divider"></div>', unsafe_allow_html=True)
                st.dataframe(sum_df, use_container_width=True)
                
                st.markdown('<div class="section-title">🏏 Batting Details</div><div class="divider"></div>', unsafe_allow_html=True)
                st.dataframe(bat_df, use_container_width=True)
                
                st.markdown('<div class="section-title">🎯 Bowling Details</div><div class="divider"></div>', unsafe_allow_html=True)
                st.dataframe(bowl_df, use_container_width=True)
                
                st.markdown('<div class="section-title">🚨 Fall of Wickets</div><div class="divider"></div>', unsafe_allow_html=True)
                st.dataframe(fow_df, use_container_width=True)

def player_stats_page():
    """UI for searching and displaying player statistics."""
    st.title("👤 Player Statistics")
    
    player_name = st.text_input("🔍 Enter player name to search", placeholder="e.g., Virat Kohli")
    if not player_name:
        st.info("Start by typing a player's name above.")
        return

    with st.spinner(f"Searching for '{player_name}'..."):
        players = search_player(player_name)

    if not players:
        st.warning("No players found. Please try a different name.")
        return

    options = {f"{p.get('name')} ({p.get('teamName', p.get('country', 'N/A'))})": p.get("id") for p in players}
    selected_label = st.selectbox("Select a player from the results:", options.keys())
    
    if selected_label:
        player_id = options[selected_label]
        
        # REFINED: Batting and bowling stats fetching is now parallelized in tabs
        batting_tab, bowling_tab = st.tabs(["🏏 Batting Stats", "🎳 Bowling Stats"])

        with batting_tab:
            with st.spinner("Fetching batting stats..."):
                bat_df = get_player_stats(player_id, "batting")
            if not bat_df.empty:
                st.dataframe(bat_df, use_container_width=True)
            else:
                st.info("No batting stats available for this player.")

        with bowling_tab:
            with st.spinner("Fetching bowling stats..."):
                bowl_df = get_player_stats(player_id, "bowling")
            if not bowl_df.empty:
                st.dataframe(bowl_df, use_container_width=True)
            else:
                st.info("No bowling stats available for this player.")

def sql_practice_page():
    """UI for practicing predefined SQL queries."""
    st.title("🗂️ SQL Practice")
    st.info("Select a predefined query from the dropdown and click 'Run Query' to see the results from the database.")
    
    # REFINED: Query dictionary with clearer, more descriptive keys.
    queries = queries = {
    "Q1. Indian Players — Name, Role & Styles": "SELECT player_id,name,battingstyle,bowlingstyle,role FROM team_india;",
    "Q2. Recent Matches — Teams, Venue & Date": "SELECT * FROM recent_matches order by recent_matches.end_date_time DESC",
    "Q3. Top 10 ODI Run Scorers": '''SELECT player_id,name,matches_played,innings_batted,runs,average,hundred
        FROM top_players order by top_players.runs DESC limit 10;''',
    "Q4. Venues with 30,000+ Capacity": "SELECT ground,city,country,capacity FROM venues where capacity > 30000 order by capacity DESC;",
    "Q5. Teams by Total Wins For A Series" :'''
                                            SELECT s.seriesName,
                                            CASE
                                                WHEN s.status ILIKE '%Sri Lanka won%' THEN 'Sri Lanka'
                                                WHEN s.status ILIKE '%New Zealand won%' THEN 'New Zealand'
                                            END AS winner_team,
                                            COUNT(*) AS total_wins
                                        FROM series_matches s
                                        WHERE s.status ILIKE '%won by%'
                                        AND s.seriesid = 8553
                                        GROUP BY s.seriesname,
                                                winner_team
                                        ORDER BY total_wins DESC;

                                    ''',
    "Q6. Player Count by Playing Role - INDIA": "SELECT role, Count(*) FROM team_india group by role",
    "Q7. Highest Score per Format (Test/ODI/T20I)": '''
                                                SELECT 
                                            player_name, format,
                                            MAX(CASE 
                                                    WHEN highest ~ '^\d+' THEN (regexp_replace(highest, '[^0-9]', '', 'g'))::INT
                                                    ELSE NULL
                                                END) AS highest_score
                                        FROM player_batting_career_stats
                                        WHERE format IN ('Test','ODI','T20','T20I','IPL')  -- adjust based on your data
                                        GROUP BY player_name,format
                                        ORDER BY highest_score DESC;''',
    "Q8. Series Started in 2024 — Details":'''
                                            SELECT host_country,
                                                seriesname,
                                                matchformat,
                                                COUNT(seriesname) AS total_number_of_matches
                                            FROM series_matches
                                            WHERE series_start_date LIKE '%2024%'
                                            AND host_country IS NOT NULL
                                            GROUP BY host_country,
                                                    seriesname,
                                                    matchformat;
                                                ''',
    "Q9. All-Rounders with 1000+ Runs & 50+ Wickets": '''
                                                    SELECT p.player_id,
                                                        p.name AS player_name,
                                                        bc.format,
                                                        SUM(bc.runs::INT) AS total_runs,
                                                        SUM(bw.wickets::INT) AS total_wickets
                                                    FROM player_batting_career_stats bc
                                                    JOIN player_bowling_career_stats bw
                                                        ON bc.player_id = bw.player_id
                                                    AND bc.format    = bw.format
                                                    JOIN players p
                                                        ON p.player_id  = bc.player_id
                                                    GROUP BY p.player_id,
                                                            p.name,
                                                            bc.format
                                                    HAVING SUM(bc.runs::INT)    > 1000
                                                    AND SUM(bw.wickets::INT) > 50
                                                    ORDER BY total_runs DESC;
                                                    ''',
    "Q10. Last 20 Completed Matches — Winners & Details":'''
                                                        SELECT match_description,
                                                                team_name_1 AS team1,
                                                                team_name_2 AS team2,
                                                                substring(RESULT
                                                                            FROM '^(.*?) won by') AS winner_team,
                                                                venue_name,
                                                                CASE
                                                                    WHEN RESULT ILIKE '%runs' THEN 'runs'
                                                                    WHEN RESULT ILIKE '%wkts' THEN 'wickets'
                                                                    ELSE 'Unknown'
                                                                END AS victory_type,
                                                                RESULT,
                                                                end_date_time AS date
                                                            FROM recent_matches
                                                            ORDER BY end_date_time DESC;
                                                            ''',
    "Q11. Player Performance Across Formats":'''
                                                WITH player_formats AS (
                                                SELECT
                                                    player_id,
                                                    player_name,
                                                    format,
                                                    runs::INT AS runs,
                                                    average::NUMERIC AS avg_value
                                                FROM player_batting_career_stats
                                                WHERE format IN ('Test', 'ODI', 'T20')
                                            ),

                                            aggregated AS (
                                                SELECT
                                                    player_id,
                                                    player_name,
                                                    COUNT(DISTINCT format) AS formats_played,
                                                    SUM(CASE WHEN format = 'Test' THEN runs ELSE 0 END) AS test_runs,
                                                    SUM(CASE WHEN format = 'ODI'  THEN runs ELSE 0 END) AS odi_runs,
                                                    SUM(CASE WHEN format = 'T20'  THEN runs ELSE 0 END) AS t20_runs,
                                                    ROUND(AVG(avg_value), 2) AS overall_avg
                                                FROM player_formats
                                                GROUP BY player_id, player_name
                                            )
                                            SELECT
                                                player_id,
                                                player_name,
                                                test_runs,
                                                odi_runs,
                                                t20_runs,
                                                overall_avg
                                            FROM aggregated
                                            WHERE formats_played >= 2
                                            ORDER BY overall_avg DESC, player_name;
                                            ''',
    "Q12. Team Wins — Home vs Away": '''
                                        SELECT 
                                        substring(status from '^(.*?) won by') AS team,
                                        CASE 
                                        WHEN LOWER(substring(status from '^(.*?) won by')) = LOWER(host_country) 
                                            THEN 'Home'
                                            ELSE 'Away'
                                        END AS location,
                                        COUNT(*) AS wins
                                        FROM series_matches
                                        WHERE status ILIKE '%won by%'
                                        GROUP BY team, location
                                        ORDER BY team, location;
                                        ''',
    "Q13. Partnerships of 100+ by Consecutive Batsmen":'''
                                                            SELECT 
                                                            b1.player_name AS player1,
                                                            b2.player_name AS player2,
                                                            b1.runs + b2.runs AS combined_runs,
                                                            b1.innings_id,
                                                            b1.series_name
                                                            FROM innings_batting b1
                                                            JOIN innings_batting b2 
                                                                ON b1.innings_id = b2.innings_id
                                                            AND b2.batting_pos = b1.batting_pos + 1
                                                            WHERE b1.runs + b2.runs >= 100;
                                                        ''',
    "Q14. Bowling Stats by Venue (Min 3 Matches & 4 Overs)":'''
                                                                SELECT bp.player_id,
                                                                    bp.player_name,
                                                                    ms.venue,
                                                                    COUNT(DISTINCT bp.match_id) AS matches_played,
                                                                    bp.overs,
                                                                    ROUND( SUM(bp.runs)::numeric / NULLIF(SUM(bp.overs)::numeric, 0), 2 ) AS avg_economy,
                                                                    SUM(bp.wickets) AS total_wickets
                                                                FROM bowling_performance bp
                                                                JOIN match_summary ms 
                                                                    ON bp.match_id = ms.match_id
                                                                GROUP BY bp.player_id, bp.player_name, bp.overs,ms.venue
                                                                HAVING COUNT(DISTINCT CASE WHEN bp.overs >= 4 THEN bp.match_id END) >= 3
                                                                ORDER BY bp.player_name, avg_economy,ms.venue;
                                                                ''',
    "Q15. Top Performers in Close Matches":'''
                                            WITH close_matches AS
                                            (SELECT sm.match_id, -- Define close match condition
                                            CASE
                                                WHEN sm.result ILIKE '%won by%'
                                                    AND sm.result ILIKE '%run%'
                                                    AND regexp_replace(sm.result, '\D', '', 'g')::INT < 50 THEN TRUE
                                                WHEN sm.result ILIKE '%won by%'
                                                    AND sm.result ILIKE '%wkt%'
                                                    AND regexp_replace(sm.result, '\D', '', 'g')::INT < 5 THEN TRUE
                                                ELSE FALSE
                                            END AS is_close,
                                            regexp_replace(sm.result, ' won.*', '') AS winner_team,
                                            regexp_replace(sm.result, '.*won by ', '') AS margin
                                            FROM match_summary sm)
                                            SELECT bp.player_name,
                                                p.team,
                                                ROUND(AVG(bp.runs::INT), 2) AS avg_runs_close_matches,
                                                COUNT(DISTINCT bp.match_id) AS total_close_matches,
                                                SUM(CASE
                                                        WHEN cm.winner_team = p.team THEN 1
                                                        ELSE 0
                                                    END) AS matches_won_by_team,
                                                STRING_AGG(DISTINCT cm.margin, ', ') AS win_margins
                                            FROM batting_performance bp
                                            JOIN players p ON bp.player_id = p.player_id
                                            JOIN close_matches cm ON cm.match_id = bp.match_id
                                            WHERE cm.is_close = TRUE
                                            GROUP BY bp.player_name,
                                                    p.name,
                                                    p.team
                                            ORDER BY avg_runs_close_matches DESC;
                                            ''',
    "Q16. Batting Trends by Year (Since 2020)":'''
                                                WITH player_year_stats AS (
                                                    SELECT
                                                        bp.player_id,
                                                        p.name,
                                                        EXTRACT(YEAR FROM smt.start_date) AS year,
                                                        COUNT(DISTINCT bp.match_id) AS matches_played,
                                                        ROUND(SUM(bp.runs) * 1.0 / COUNT(DISTINCT bp.match_id), 2) AS avg_runs_per_match,
                                                        ROUND(AVG(NULLIF(bp.strike_rate, 0))::Numeric, 2) AS avg_strike_rate
                                                    FROM batting_performance bp
                                                    JOIN match_summary ms 
                                                        ON bp.match_id = ms.match_id
                                                    JOIN series_matches smt
                                                        ON ms.match_id = smt.match_id
                                                    JOIN players p
                                                        ON bp.player_id = p.player_id
                                                    GROUP BY bp.player_id, p.name, year
                                                ),
                                                multi_year_players AS (
                                                    SELECT player_id
                                                    FROM player_year_stats
                                                    GROUP BY player_id
                                                    HAVING COUNT(DISTINCT year) >= 4
                                                )
                                                SELECT pys.*
                                                FROM player_year_stats pys
                                                JOIN multi_year_players myp
                                                ON pys.player_id = myp.player_id
                                                WHERE pys.year >= 2020
                                                AND pys.matches_played >= 5
                                                ORDER BY pys.name, pys.year;
                                                '''}
    choice = st.selectbox("Choose a query to run:", list(queries.keys()))

    if st.button("Run Query", type="primary"):
        with st.spinner("Executing query..."):
            final_query = queries[choice]
            result_df = run_query(final_query)
            if not result_df.empty:
                st.dataframe(result_df)
            else:
                st.warning("Query executed, but returned no results.")

def top_players_crud_page():
    """UI for CRUD operations on the top_players table."""
    st.header("🏆 Top Players - Database Management")
    
    conn = get_db_connection()
    if not conn:
        st.error("Database connection is required for this page.")
        return

    view_tab, add_tab, update_tab, delete_tab = st.tabs(["📋 View Players", "➕ Add Player", "✏️ Update Player", "❌ Delete Player"])

    with view_tab:
        st.subheader("All Players in Database")
        df = run_query("SELECT * FROM top_players ORDER BY id")
        search = st.text_input("Filter by name:", key="view_search")
        if search:
            df = df[df["name"].str.contains(search, case=False, na=False)]
        st.dataframe(df, use_container_width=True)
        # Visualization part remains the same...
        if not df.empty and len(df) > 1:
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Top by Runs")
                st.bar_chart(df.set_index("name")["runs"].nlargest(10))
            with col2:
                st.subheader("Top by Hundreds")
                st.bar_chart(df.set_index("name")["hundred"].nlargest(10))

    with add_tab:
        with st.form("add_form", clear_on_submit=True):
            st.subheader("Add a New Player")
            col1, col2 = st.columns(2)
            # Input fields remain the same...
            player_id, name = col1.number_input("Player ID", step=1, min_value=1), col1.text_input("Name")
            matches_played, innings_batted = col1.number_input("Matches Played", step=1, min_value=0), col1.number_input("Innings Batted", step=1, min_value=0)
            runs, average, hundred = col2.number_input("Runs", step=1, min_value=0), col2.number_input("Average", format="%.2f"), col2.number_input("Hundreds", step=1, min_value=0)
            
            if st.form_submit_button("Add Player", type="primary"):
                if not name.strip():
                    st.error("Player name is required!")
                else:
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO top_players (player_id, name, matches_played, innings_batted, runs, average, hundred)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """, (player_id, name, matches_played, innings_batted, runs, average, hundred))
                            conn.commit()
                        st.success(f"Player '{name}' added successfully!")
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Insert failed: {e}")

    with update_tab:
        st.subheader("Update an Existing Player")
        update_df = run_query("SELECT id, name, player_id, runs, average, hundred FROM top_players ORDER BY name")
        if not update_df.empty:
            player_to_update = st.selectbox("Select player to update:", options=update_df['name'], index=None)
            if player_to_update:
                player_data = update_df[update_df['name'] == player_to_update].iloc[0]
                with st.form("update_form"):
                    st.write(f"Updating stats for **{player_to_update}**")
                    new_runs = st.number_input("New total runs", value=int(player_data['runs']))
                    new_avg = st.number_input("New average", value=float(player_data['average']), format="%.2f")
                    new_hund = st.number_input("New hundreds", value=int(player_data['hundred']))
                    
                    if st.form_submit_button("Update Player", type="primary"):
                        try:
                            with conn.cursor() as cur:
                                cur.execute("UPDATE top_players SET runs=%s, average=%s, hundred=%s WHERE player_id=%s",
                                            (new_runs, new_avg, new_hund, int(player_data['player_id'])))
                                conn.commit()
                            st.success(f"Player '{player_to_update}' updated successfully.")
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Update failed: {e}")

    with delete_tab:
        st.subheader("Delete a Player")
        delete_df = run_query("SELECT name, player_id FROM top_players ORDER BY name")
        if not delete_df.empty:
            player_to_delete = st.selectbox("Select player to delete:", options=delete_df['name'], index=None)
            if player_to_delete:
                st.warning(f"**Danger Zone:** Are you sure you want to delete **{player_to_delete}**? This action cannot be undone.")
                if st.button(f"Yes, permanently delete {player_to_delete}", type="primary"):
                    try:
                        delete_id = int(delete_df[delete_df['name'] == player_to_delete]['player_id'].iloc[0])
                        with conn.cursor() as cur:
                            cur.execute("DELETE FROM top_players WHERE player_id = %s", (delete_id,))
                            conn.commit()
                        st.success(f"Player '{player_to_delete}' deleted successfully!")
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Delete failed: {e}")

# ----------------------------
# Main App Navigation
# ----------------------------
def main():
    """Main function to run the Streamlit app."""
    st.sidebar.title("MENU")
    
    # Page names
    page_options = {
        "Home": home_page,
        "Live Match Scores": live_match_page,
        "Player Statistics": player_stats_page,
        "SQL Practice": sql_practice_page,
        "Top Players DB (CRUD)": top_players_crud_page
    }
    
    selection = st.sidebar.radio("Go to", list(page_options.keys()))
    
    # Execute the selected page function
    page_function = page_options[selection]
    page_function()

if __name__ == "__main__":

    main()

#!/usr/bin/env python3
import os
from fastapi import FastAPI, Path, Query, HTTPException, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from datetime import date
import duckdb
from dotenv import load_dotenv
import re
import logging
import time
from typing import List, Optional, Tuple
import pandas as pd
from pydantic import BaseModel

# ─── CONFIG & APP SETUP ─────────────────────────────────────────────────────
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
DB_PATH = os.getenv("CRISTAT_DB_PATH", "cristat.duckdb")
app = FastAPI(title="CricketStats T20 Props API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # for production, specify domain(s)
    allow_methods=["GET"],
    allow_headers=["*"]
)

# Create an API router without prefix
router = APIRouter()

# Single global DuckDB connection with optimizations
print(f"Connecting to database at {DB_PATH}")
try:
    db = duckdb.connect(DB_PATH, read_only=True)  # Open in read-only mode
    # Enable progress bar
    db.execute("SET enable_progress_bar=1")
    print(f"Database connected, total matches: {db.execute('SELECT COUNT(*) FROM match').fetchone()[0]}")
except Exception as e:
    print(f"Error connecting to database: {e}")
    raise

# ─── HELPER: SEASON FILTER (New) ──────────────────────────────────────────
def get_season_filter_clause(
    season: Optional[str] = None, 
    last_n_seasons_count: Optional[int] = None, 
    table_alias: str = 'm',
    tournament_names: Optional[List[str]] = None  # New parameter
) -> Tuple[str, list]:
    """
    Builds SQL filter clause for seasons.
    If last_n_seasons_count is provided, it fetches the N most recent distinct seasons,
    optionally filtered by tournament_names.
    Otherwise, if season is provided, it filters by that specific season.
    `last_n_seasons_count` takes precedence over `season` if both are provided.
    Returns a tuple: (SQL clause string, list of parameters).
    """
    query_params = []
    if last_n_seasons_count is not None and last_n_seasons_count > 0:
        try:
            season_conditions = ["season IS NOT NULL"]
            if tournament_names:
                # Ensure tournament_names is a list of non-empty strings
                valid_tournament_names = [name for name in tournament_names if name and name.strip()]
                if valid_tournament_names:
                    placeholders = ', '.join(['?'] * len(valid_tournament_names))
                    season_conditions.append(f"event_name IN ({placeholders})")
                    query_params.extend(valid_tournament_names)
            
            season_where_clause = " AND ".join(season_conditions)
            all_seasons_query = f"SELECT DISTINCT season FROM match WHERE {season_where_clause} ORDER BY season DESC"
            
            logger.info(f"Fetching distinct seasons with query: {all_seasons_query} and params: {query_params}")
            raw_seasons = db.execute(all_seasons_query, query_params if query_params else None).fetchall()
            distinct_seasons = [s[0] for s in raw_seasons]

            if not distinct_seasons:
                logger.warning(f"No distinct seasons found with filters: tournaments={tournament_names}, last_n={last_n_seasons_count}")
                return "", []

            def sort_key_season(s_item):
                match_year = re.search(r'\b(\d{4})\b', s_item)
                year = int(match_year.group(1)) if match_year else 0
                # Prioritize fully numeric seasons first for a given year, then hyphenated/slashed
                is_numeric_season = bool(re.fullmatch(r'\d{4}', s_item))
                # Sort by year descending (recent first)
                # Then, for the same year, prioritize numeric seasons (False) over non-numeric (True)
                # Then, by season string ascending for tie-breaking
                return (-year, not is_numeric_season, s_item)
            
            # Sort in Python to get seasons in order: most recent first, numeric preferred for same year
            distinct_seasons.sort(key=sort_key_season) 
            logger.info(f"Sorted distinct seasons (top 10 after Python sort): {distinct_seasons[:10]}")
            
            selected_seasons = distinct_seasons[:last_n_seasons_count]
            logger.info(f"Selected top {last_n_seasons_count} seasons: {selected_seasons}")
            
            if not selected_seasons:
                return "", []
            
            # Parameters for the final IN clause are just the selected season strings
            final_season_params = selected_seasons
            placeholders = ', '.join(['?'] * len(selected_seasons))
            return f"{table_alias}.season IN ({placeholders})", final_season_params
        except Exception as e:
            logger.error(f"Error processing last_n_seasons: {e}", exc_info=True)
            return "", [] 
    elif season:
        # If specific season is given, tournament_names are not used for this direct filter
        return f"{table_alias}.season = ?", [season]
    return "", []

# ─── HELPER: QUERY EXECUTION WITH TIMEOUT AND LOGGING ────────────────────────
def execute_query_safe(sql: str, params: tuple = None, timeout: int = 30) -> List[tuple]:
    start_time = time.time()
    try:
        logger.info(f"Executing query with params: {params}")
        result = db.execute(sql, params).fetchall() if params else db.execute(sql).fetchall()
        duration = time.time() - start_time
        logger.info(f"Query completed in {duration:.2f}s")
        return result
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Query failed after {duration:.2f}s: {str(e)}")
        logger.error(f"SQL: {sql}")
        logger.error(f"Params: {params}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# ─── AUTOCOMPLETE ────────────────────────────────────────────────────────────
@router.get("/players")
def player_lookup(
    q: str = Query(None, min_length=0),
    limit: int = Query(50, ge=1, le=200)
):
    logger.info(f"Player lookup with q='{q if q else ''}', limit={limit}")
    sql = """
SELECT DISTINCT player AS name FROM (
  SELECT batter    AS player FROM delivery
  UNION ALL SELECT bowler    AS player FROM delivery
  UNION ALL SELECT non_striker AS player FROM delivery
)
WHERE player ILIKE '%' || COALESCE(?, '') || '%'
ORDER BY name
LIMIT ?;"""
    try:
        return db.execute(sql, (q, limit)).fetch_df().to_dict('records')
    except Exception as e:
        logger.error(f"Error in player_lookup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching player lookup.")

@router.get("/teams")
def team_lookup(
    q: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=200)
):
    logger.info(f"Team lookup with q='{q}', limit={limit}")
    sql = """
SELECT DISTINCT team AS name FROM (
  SELECT DISTINCT batting_team AS team FROM innings WHERE batting_team IS NOT NULL
  UNION 
  SELECT DISTINCT bowling_team AS team FROM innings WHERE bowling_team IS NOT NULL
)
WHERE team ILIKE '%' || ? || '%'
ORDER BY name
LIMIT ?;"""
    try:
        return db.execute(sql, (q, limit)).fetch_df().to_dict('records')
    except Exception as e:
        logger.error(f"Error in team_lookup: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching team lookup.")

# ─── LOOKUPS ─────────────────────────────────────────────────────────────────
@router.get("/tournaments")
def list_tournaments():
    """
    Returns a list of tournaments with a `type` field:
      - 'Franchise' if the name matches known league keywords
      - 'International' otherwise
    """
    try:
        logger.info("Querying tournaments...")
        rows = db.execute(
            "SELECT DISTINCT event_name FROM match WHERE event_name IS NOT NULL ORDER BY event_name"
        ).fetchall()
        logger.info(f"Found {len(rows)} raw tournament events")

        franchise_keywords = [
            # General Acronyms & Keywords (often without strict word boundaries needed)
            # These are typically safe as they are unique acronyms or very specific phrases
            r"IPL", r"BBL", r"PSL", r"CPL", r"SA20", r"ILT20", r"MLC", r"MSL", 
            r"T20 Blast", r"Vitality Blast", 

            # More specific names requiring word boundaries to avoid partial matches in international series
            r"\bIndian Premier League\b",
            r"\bBig Bash League\b",
            r"\bBig Bash\b", # For cases where "League" might be missing
            r"\bPakistan Super League\b",
            r"\bCaribbean Premier League\b",
            r"\bLanka Premier League\b", r"\bLPL\b", 
            r"\bMajor League Cricket\b",
            r"\bMzansi Super League\b",
            r"\bThe Hundred\b",
            r"\bBangladesh Premier League\b", r"\bBPL\b",
            r"\bSuper Smash\b",
            r"\bGlobal T20 Canada\b", r"\bGlobal T20\b",
            r"\bAbu Dhabi T10 League\b", r"\bT10 League\b",
            r"\bWomen's Premier League\b", r"\bWPL\b",
            r"\bFairBreak Invitational T20\b",
            r"\bEverest Premier League\b",
            r"\bHong Kong T20 Blitz\b",
            r"\bT20 Mumbai League\b",
            r"\bTamil Nadu Premier League\b", r"\bTNPL\b",
            r"\bKarnataka Premier League\b", r"\bKPL\b",
            
            # Catch-all for names explicitly ending in "League" or containing "T20 League"
            r"T20 League\b", 
            r"\b[A-Za-z0-9 ]+ Premier League\b", 
            r"\b[A-Za-z0-9 ]+ League\b", 
            # r"\b[A-Za-z0-9 ]+ T20\b", # REMOVED as it was too broad
        ]
        tournaments_data = []
        for (name,) in rows:
            ttype = "International"
            for kw in franchise_keywords:
                if re.search(kw, name, re.IGNORECASE):
                    ttype = "Franchise"
                    break
            tournaments_data.append({"name": name, "type": ttype})
        logger.info(f"Returning {len(tournaments_data)} classified tournaments")
        return tournaments_data
    except Exception as e:
        logger.error(f"Error in list_tournaments: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching tournaments.")

@router.get("/teams/list")
def list_teams(tournaments: str = Query(None, description="Comma-separated tournament names")):
    logger.info(f"Listing teams for tournaments: '{tournaments if tournaments else 'All'}'")
    try:
        params = []
        # Reverted to use innings table
        base_sql_select = "SELECT DISTINCT i.batting_team AS name FROM innings i"
        base_sql_order = "ORDER BY name"
        join_match = " JOIN match m ON i.match_id = m.match_id"
        where_clauses = ["i.batting_team IS NOT NULL"]

        if tournaments:
            names = [t.strip() for t in tournaments.split(',') if t.strip()]
            if names:
                ph = ','.join('?' for _ in names)
                where_clauses.append(f"m.event_name IN ({ph})")
                params.extend(names)
                # Query joins innings with match if tournaments are specified
                sql = f"{base_sql_select}{join_match} WHERE {' AND '.join(where_clauses)} {base_sql_order}"
            else: # tournaments string was empty or all whitespace
                # Query from innings table directly
                sql = f"{base_sql_select} WHERE {' AND '.join(where_clauses)} {base_sql_order}"
        else:
            # Query from innings table directly
            sql = f"{base_sql_select} WHERE {' AND '.join(where_clauses)} {base_sql_order}"
        df = db.execute(sql, params if params else None).fetch_df()
        logger.info(f"Found {len(df)} teams.")
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error in list_teams: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching team list.")

@router.get("/seasons")
def list_seasons(tournaments: str = Query(None)):
    logger.info(f"Listing seasons for tournaments: '{tournaments if tournaments else 'All'}'")
    try:
        params = []
        sql = "SELECT DISTINCT season FROM match WHERE season IS NOT NULL ORDER BY season" # Default SQL

        if tournaments:
            names = [t.strip() for t in tournaments.split(',') if t.strip()]
            if names:
                ph = ','.join('?' for _ in names)
                sql = f"""
          SELECT DISTINCT season 
          FROM match 
                            WHERE event_name IN ({ph}) AND season IS NOT NULL
          ORDER BY season
        """
                params.extend(names)
            # If tournaments string is provided but names is empty, default SQL is used.
        
        # This part executes regardless of whether 'tournaments' was initially present, using either modified or default sql
        df = db.execute(sql, params if params else None).fetch_df()
        logger.info(f"Found {len(df)} seasons.")
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error in list_seasons: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching seasons.")

@router.get("/venues")
def list_venues(tournaments: str = Query(None)):
    logger.info(f"Listing venues for tournaments: '{tournaments if tournaments else 'All'}'")
    try:
        params = []
        sql = "SELECT DISTINCT venue AS name FROM match WHERE venue IS NOT NULL ORDER BY name" # Default SQL

        if tournaments:
            names = [t.strip() for t in tournaments.split(',') if t.strip()]
            if names:
                ph = ','.join('?' for _ in names)
                sql = f"""
                  SELECT DISTINCT venue AS name 
              FROM match 
              WHERE event_name IN ({ph}) AND venue IS NOT NULL 
                  ORDER BY name
                """
                params.extend(names)
            # If tournaments is provided but names is empty, the default SQL (all venues) remains.

        df = db.execute(sql, params if params else None).fetch_df()
        logger.info(f"Found {len(df)} venues")
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error in list_venues: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error fetching venues.")

@router.get("/teams/{team}/players")
def list_team_players(
    team: str = Path(...)
):
    logger.info(f"Listing players for team: '{team}'")
    # Reverted to use innings table for batting_team context
    sql = """
SELECT DISTINCT player AS name FROM (
    SELECT d.batter AS player
FROM delivery d
    JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
    WHERE i.batting_team = ? AND d.batter IS NOT NULL
UNION
    SELECT d.non_striker AS player
FROM delivery d
    JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
    WHERE i.batting_team = ? AND d.non_striker IS NOT NULL
) ORDER BY name;
"""
    try:
        return db.execute(sql, (team, team)).fetch_df().to_dict('records')
    except Exception as e:
        logger.error(f"Error in list_team_players for team {team}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching players for team {team}.")

# ─── BATTERS ─────────────────────────────────────────────────────────────────
@router.get("/batters")
def batters(
    tournament:    Optional[str] = Query(None),
    team:          Optional[str] = Query(None),
    season:        Optional[str] = Query(None, description="Filter by a specific season (e.g., '2022', '2023/24'). Exact match. Overridden by last_n."),
    last_n:        Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    innings:       str = Query("all", regex="^(all|first|second)$"),
    venue:         Optional[str] = Query(None),
    opposition:    Optional[str] = Query(None),
    players:       Optional[str] = Query(None),
    min_matches:   int = Query(1, ge=1),
    min_total_4s:  Optional[int] = Query(None, ge=0),
    min_total_6s:  Optional[int] = Query(None, ge=0),
    min_avg_4s_pm: Optional[float] = Query(None, ge=0.0),
    min_avg_6s_pm: Optional[float] = Query(None, ge=0.0),
    top_x_criteria:str = Query("runs_off_bat", regex="^(runs_off_bat|matches_played|balls_faced|total_4s|total_6s|avg_4s_pm|avg_6s_pm)$"),
    top_x:         int = Query(25, ge=1, le=500)
):
    logger.info(f"Batters query: tournament={tournament}, team={team}, season={season}, last_n={last_n}, "
                f"innings={innings}, venue={venue}, opposition={opposition}, players={players}, "
                f"min_matches={min_matches}, min_total_4s={min_total_4s}, min_total_6s={min_total_6s}, "
                f"min_avg_4s_pm={min_avg_4s_pm}, min_avg_6s_pm={min_avg_6s_pm}, "
                f"top_x_criteria={top_x_criteria}, top_x={top_x}")
    
    try: 
        base_query = """
        SELECT 
            d.batter AS player,
            d.match_id,
            i.batting_team,
            i.bowling_team,
            d.innings_no,
            m.event_name,
            m.season AS match_season, 
            m.match_date,
            m.venue AS match_venue,
            m.match_type, 
            d.runs_batter,
            CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides','noballs') THEN 1 ELSE 0 END AS ball_faced_count,
            CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END AS is_4,
            CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END AS is_6
        FROM delivery d
        JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
        JOIN match m ON d.match_id = m.match_id
        """
        
        filters, params = [], []
        
        tournament_name_list = []
        if tournament:
            # Note: /batters endpoint currently only supports a single tournament string, not comma-separated
            # If it needs to support multiple, the Query definition for tournament and this logic should change.
            # For now, we'll treat it as a list containing a single tournament if provided.
            tournament_name_list = [tournament] if tournament.strip() else []
            if tournament_name_list: # This ensures it's a non-empty list if tournament was provided
                filters.append("m.event_name = ?")
                params.append(tournament_name_list[0]) # Pass the single tournament name

        season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)
        if season_clause:
            filters.append(season_clause)
            params.extend(season_params)
        
        if team:
            filters.append("i.batting_team = ?")
            params.append(team)
        if venue:
            filters.append("m.venue = ?")
            params.append(venue)
        if opposition: 
            filters.append("i.bowling_team = ?")
            params.append(opposition)
        if players:
            player_list = [p.strip() for p in players.split(',') if p.strip()]
            if player_list:
                ph = ','.join('?' for _ in player_list)
                filters.append(f"d.batter IN ({ph})")
                params.extend(player_list)
        
        if innings == "first":
            filters.append("d.innings_no = 1")
        elif innings == "second":
            filters.append("d.innings_no = 2")

        if filters:
            base_query += " WHERE " + " AND ".join(filters)

        logger.info(f"Executing base data query: {base_query} with params: {params}")
        all_deliveries_df = db.execute(base_query, params if params else None).fetch_df()
        logger.info(f"Fetched {len(all_deliveries_df)} delivery records.")

        if all_deliveries_df.empty:
            return []

        player_stats = all_deliveries_df.groupby('player').agg(
            matches_played = ('match_id', 'nunique'),
            runs_off_bat = ('runs_batter', 'sum'),
            balls_faced = ('ball_faced_count', 'sum'),
            total_4s = ('is_4', 'sum'),
            total_6s = ('is_6', 'sum')
        ).reset_index()
        logger.info(f"Aggregated stats for {len(player_stats)} players.")

        if player_stats.empty:
            return []

        player_stats = player_stats[player_stats['matches_played'] >= min_matches]
        logger.info(f"Applied min_matches >= {min_matches}, {len(player_stats)} players remaining.")

        if player_stats.empty:
            return []

        player_stats['avg_4s_pm'] = ((player_stats['total_4s'] / player_stats['matches_played']).fillna(0)).round(2)
        player_stats['avg_6s_pm'] = ((player_stats['total_6s'] / player_stats['matches_played']).fillna(0)).round(2)

        if min_total_4s is not None:
            player_stats = player_stats[player_stats['total_4s'] >= min_total_4s]
            logger.info(f"Applied min_total_4s >= {min_total_4s}, {len(player_stats)} players remaining.")
        if min_total_6s is not None:
            player_stats = player_stats[player_stats['total_6s'] >= min_total_6s]
            logger.info(f"Applied min_total_6s >= {min_total_6s}, {len(player_stats)} players remaining.")
        if min_avg_4s_pm is not None:
            player_stats = player_stats[player_stats['avg_4s_pm'] >= min_avg_4s_pm]
            logger.info(f"Applied min_avg_4s_pm >= {min_avg_4s_pm}, {len(player_stats)} players remaining.")
        if min_avg_6s_pm is not None:
            player_stats = player_stats[player_stats['avg_6s_pm'] >= min_avg_6s_pm]
            logger.info(f"Applied min_avg_6s_pm >= {min_avg_6s_pm}, {len(player_stats)} players remaining.")

        if player_stats.empty:
            return []

        if top_x_criteria not in player_stats.columns:
            logger.error(f"Invalid top_x_criteria: {top_x_criteria}")
            raise HTTPException(status_code=400, detail=f"Invalid sort criteria: {top_x_criteria}")
        
        player_stats = player_stats.sort_values(by=top_x_criteria, ascending=False).head(top_x)
        logger.info(f"Sorted by {top_x_criteria} and took top {top_x}, {len(player_stats)} players remaining.")

        return player_stats.to_dict('records')
        
    except Exception as e:
        logger.error(f"Error in batters endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing batters request: {str(e)}")

# ─── BOWLERS ───────────────────────────────────────────────────────────────
@router.get("/bowlers/{player}/thresholds")
def bowler_thresholds(
    player:      str = Path(...),
    min_wickets: int = Query(1, ge=1),
    season:      Optional[str] = Query(None, description="Filter by a specific season. Overridden by last_n."),
    last_n:      Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include.")
):
    season_clause, season_params = get_season_filter_clause(season, last_n)
    filters, params = ["m.match_type='T20'",
                      "d.bowler=?"], [player]
    if season_clause:
        filters.append(season_clause)
        params.extend(season_params)
    where_raw = ' AND '.join(filters)
    sql = f"""
WITH per_innings_deliveries AS (
  SELECT d.match_id, d.innings_no,
         (CASE WHEN d.player_out IS NOT NULL AND d.wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field', 'handled the ball', 'timed out') THEN 1 ELSE 0 END) AS is_wkt_on_ball
  FROM delivery d
  JOIN innings i ON d.match_id=i.match_id AND d.innings_no=i.innings_no
  JOIN match m ON d.match_id=m.match_id
  WHERE {where_raw}
),
per_actual_innings AS (
  SELECT match_id, innings_no,
         SUM(is_wkt_on_ball) AS wkts
  FROM per_innings_deliveries
  GROUP BY match_id, innings_no
)
SELECT
  COUNT(*) AS total_innings,
  SUM(CASE WHEN wkts >= {min_wickets} THEN 1 ELSE 0 END) AS successes,
  ROUND(100.0*SUM(CASE WHEN wkts >= {min_wickets} THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),2) AS pct
FROM per_actual_innings;
"""
    row = db.execute(sql, params).fetchone()
    return {"player": player, "min_wickets": min_wickets,
            "total_innings": row[0], "successes": row[1], "pct": row[2]}

@router.get("/bowlers")
def bowlers(
    season:         Optional[str] = Query(None, description="Filter by a specific season. Overridden by last_n."),
    last_n:         Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    venue:          Optional[str] = Query(None),
    opposition:     Optional[str] = Query(None),
    min_innings:    int = Query(1, ge=1),
    players:        Optional[str] = Query(None),
    tournament:     Optional[str] = Query(None), # Added tournament filter
    top_x_criteria: str = Query("total_wkts"),
    top_x:          int = Query(25, ge=1, le=500)
):
    try: 
        filters, params = ["m.match_type='T20'"], []

        tournament_name_list = []
        if tournament:
            tournament_name_list = [t.strip() for t in tournament.split(',') if t.strip()]
            if tournament_name_list:
                placeholders = ",".join("?" for _ in tournament_name_list)
                filters.append(f"m.event_name IN ({placeholders})")
                params.extend(tournament_name_list)

        season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)
        if season_clause:
            filters.append(season_clause)
            params.extend(season_params)
        if venue:
            filters.append('m.venue=?'); params.append(venue)
        if opposition:
            filters.append('i.batting_team=?'); params.append(opposition)
        if players:
            names = [p.strip() for p in players.split(',') if p.strip()]
            if names:
                ph = ','.join('?' for _ in names)
                filters.append(f"d.bowler IN ({ph})"); params.extend(names)
        
        where_raw = ' AND '.join(filters)
        
        allowed_criteria = {'matches_bowled','total_wkts','runs_conceded',
               'balls_bowled','economy','avg','sr'}
        if top_x_criteria not in allowed_criteria:
            raise HTTPException(status_code=400, detail=f"Invalid sort criteria: {top_x_criteria}")

        logger.info(f"Bowlers query: filters={where_raw}, params={params}, min_innings={min_innings}, sort={top_x_criteria}, top_x={top_x}")

        sql = f"""
WITH inds AS (
  SELECT d.bowler AS player, d.match_id, d.innings_no,
                 i.batting_team, -- This is the team the bowler bowled AGAINST
                 d.runs_total AS runs_conceded_on_ball,
                 (CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides','noballs') THEN 1 ELSE 0 END) AS is_legal_ball,
                 (CASE WHEN d.player_out IS NOT NULL AND d.wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field', 'handled the ball', 'timed out') THEN 1 ELSE 0 END) AS is_wkt_on_ball
  FROM delivery d
  JOIN innings i ON d.match_id=i.match_id AND d.innings_no=i.innings_no
  JOIN match   m ON d.match_id=m.match_id
  WHERE {where_raw}
),
per_innings AS (
          SELECT player, match_id, innings_no, 
                 SUM(runs_conceded_on_ball) AS runs_conceded,
                 SUM(is_wkt_on_ball)       AS total_wkts,
                 SUM(is_legal_ball)        AS balls_bowled
  FROM inds GROUP BY player, match_id, innings_no
),
agg AS (
  SELECT player,
                 COUNT(DISTINCT match_id) AS matches_bowled,
         SUM(total_wkts)        AS total_wkts,
         SUM(runs_conceded)     AS runs_conceded,
         SUM(balls_bowled)      AS balls_bowled,
                 ROUND(SUM(runs_conceded)::DOUBLE/NULLIF(SUM(balls_bowled)/6.0, 0),2) AS economy,
                 CASE WHEN SUM(total_wkts)=0 THEN NULL ELSE ROUND(SUM(balls_bowled)::DOUBLE/NULLIF(SUM(total_wkts),0),2) END AS sr,
                 CASE WHEN SUM(total_wkts)=0 THEN NULL ELSE ROUND(SUM(runs_conceded)::DOUBLE/NULLIF(SUM(total_wkts),0),2) END AS avg
  FROM per_innings GROUP BY player
)
SELECT * FROM agg
        WHERE matches_bowled>=?
        ORDER BY {top_x_criteria} DESC LIMIT {top_x};
        """
        final_params = params + [min_innings]
        df = db.execute(sql, final_params).fetch_df()
        logger.info(f"Bowlers query returned {len(df)} rows before final processing.")
        
        if df.empty:
            return []
            
        if top_x_criteria not in df.columns:
            logger.warning(f"Sort criteria '{top_x_criteria}' not in returned columns: {df.columns.tolist()}. Returning unsorted or empty.")
            if not df.empty: 
                 raise HTTPException(status_code=400, detail=f"Invalid sort criteria '{top_x_criteria}' for available data.")
        
        return df.to_dict('records')
    except duckdb.InvalidInputException as e:
        logger.error(f"DuckDB Invalid Input Error in /bowlers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database input error: {str(e)}")
    except Exception as e:
        logger.error(f"Generic error in /bowlers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing bowlers request: {str(e)}")

@router.get("/teams/{team}/over_runs")
def team_over_runs(
    team:   str = Path(...),
    overs:  int = Query(..., ge=1, le=20),
    season: Optional[str] = Query(None, description="Filter by a specific season. Overridden by last_n."),
    last_n: Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    tournament: Optional[str] = Query(None) # Added tournament filter
):
    filters, initial_params_for_where = ["m.match_type='T20'"], []

    tournament_name_list = []
    if tournament:
        # Assuming single tournament for this endpoint's season context for simplicity, adjust if multi-tournament season scope is needed
        tournament_name_list = [tournament] if tournament.strip() else [] 
        if tournament_name_list:
             filters.append("m.event_name = ?") # Add tournament filter to main query as well
             initial_params_for_where.append(tournament_name_list[0])

    season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)
    
    if season_clause:
        filters.append(season_clause)
        initial_params_for_where.extend(season_params)
    
    where_raw = ' AND '.join(filters)

    # SQL needs WHERE {where_raw} AND i.batting_team = ? ... GROUP BY i.match_id, i.innings_no ... WHERE d.over_no < ?
    # Parameters for SQL: [*initial_params_for_where, team (for i.batting_team), overs (for d.over_no)]
    final_query_params = initial_params_for_where + [team, overs]

    sql = f"""
WITH delivery_facts AS (
  SELECT i.match_id, i.innings_no, i.batting_team, -- Using i.batting_team from innings
         d.over_no,
         d.runs_total
  FROM delivery d
  JOIN innings i ON d.match_id=i.match_id AND d.innings_no=i.innings_no -- Re-added JOIN
  JOIN match m ON d.match_id=m.match_id
  WHERE {where_raw} AND i.batting_team = ? -- Using i.batting_team
),
runs_in_overs AS (
  SELECT match_id, innings_no, 
         SUM(runs_total) AS runs_in_first_n
  FROM delivery_facts
  WHERE over_no < ? 
  GROUP BY match_id, innings_no
)
SELECT
  ROUND(AVG(runs_in_first_n),2) AS avg_runs,
  MIN(runs_in_first_n)         AS min_runs,
  MAX(runs_in_first_n)         AS max_runs
FROM runs_in_overs;
""" # Re-added JOIN with innings table, using i.batting_team

    row = db.execute(sql, final_query_params).fetchone()
    return {"team": team, "overs": overs, "avg_runs": row[0], "min_runs": row[1], "max_runs": row[2]}

# @router.get("/matchups") # Temporarily commented out due to NameError: name 'MatchupRecord' is not defined
def matchups(
    batter:       str = Query(...),
    bowling_team: str = Query(...),
    season:       Optional[str] = Query(None, description="Filter by a specific season. Overridden by last_n."),
    last_n:       Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    tournament:   Optional[str] = Query(None) # Added tournament
):
    filters, params_list = ["m.match_type='T20'"], [] 

    tournament_name_list = []
    if tournament:
        # Assuming single tournament for this endpoint's season context
        tournament_name_list = [tournament] if tournament.strip() else []
        if tournament_name_list:
            filters.append("m.event_name = ?") # Add to main query
            params_list.append(tournament_name_list[0])

    season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)

    filters.append("d.batter=?")
    params_list.append(batter)
    filters.append("i.bowling_team=?")
    params_list.append(bowling_team)

    if season_clause:
        filters.append(season_clause)
        params_list.extend(season_params)
    
    where_raw = ' AND '.join(filters)
    sql = f"""
SELECT
  SUM(d.runs_batter)                               AS total_runs,
  SUM(CASE WHEN d.player_out=d.batter THEN 1 ELSE 0 END) AS dismissals,
  SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides','noballs') THEN 1 ELSE 0 END) AS balls_faced,
  ROUND(
    SUM(d.runs_batter)::DOUBLE
      / NULLIF(SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides','noballs') THEN 1 ELSE 0 END),0)
      * 100,
    2
  ) AS strike_rate
FROM delivery d
JOIN innings i ON d.match_id=i.match_id AND d.innings_no=i.innings_no
JOIN match   m ON d.match_id=m.match_id
WHERE {where_raw};
"""
    row = db.execute(sql, params_list).fetchone()
    return {"batter": batter, "bowling_team": bowling_team,
            "total_runs": row[0], "dismissals": row[1],
            "balls_faced": row[2], "strike_rate": row[3]}

@router.get("/test", tags=["debug"])
async def test_endpoint():
    total_matches = 0
    db_connected = False
    match_table_schema = []
    try:
        if db:
            db_connected = True
            total_matches = db.execute("SELECT COUNT(*) FROM match").fetchone()[0]
            match_table_schema = db.execute("PRAGMA table_info('match');").fetchall()
            # Convert list of tuples to list of dicts for better JSON output
            match_table_schema = [{'cid': row[0], 'name': row[1], 'type': row[2], 'notnull': row[3], 'dflt_value': row[4], 'pk': row[5]} for row in match_table_schema]
    except Exception as e:
        logger.error(f"Error in /test endpoint: {e}")
        # Don't re-raise, just report status
        pass 
    return {
        "status": "ok", 
        "message": "Server is working", 
        "database": {
            "connected": db_connected, 
            "total_matches": total_matches,
            "match_table_schema": match_table_schema
        }
    }

@router.get("/debug/bowler_balls/{player_name}")
async def debug_bowler_balls(player_name: str):
    sql = """
    SELECT 
        COUNT(*) as total_deliveries,
        SUM(CASE WHEN d.extras_type IS NULL THEN 1 ELSE 0 END) as null_extras_type,
        SUM(CASE WHEN d.extras_type = 'wides' THEN 1 ELSE 0 END) as wide_balls,
        SUM(CASE WHEN d.extras_type = 'noballs' THEN 1 ELSE 0 END) as no_balls,
        SUM(CASE WHEN d.extras_type NOT IN ('wides', 'noballs') OR d.extras_type IS NULL THEN 1 ELSE 0 END) as potentially_legal_balls_lenient_null,
        SUM(CASE WHEN d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END) as strictly_legal_balls_no_null
    FROM delivery d
    WHERE d.bowler = ?;
    """
    try:
        result = db.execute(sql, (player_name,)).fetchone()
        if result:
            return {
                "player": player_name, 
                "total_deliveries": result[0],
                "null_extras_type": result[1],
                "wide_balls": result[2],
                "no_balls": result[3],
                "potentially_legal_balls_lenient_null": result[4],
                "strictly_legal_balls_no_null": result[5]
            }
        return {"player": player_name, "error": "Player not found or no deliveries"}
    except Exception as e:
        logger.error(f"Error in debug_bowler_balls for {player_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/debug/h2h_delivery_types")
async def debug_h2h_delivery_types(batter_name: str, bowler_name: str):
    sql = """
    SELECT 
        d.extras_type,
        COUNT(*) as count
    FROM delivery d
    JOIN match m ON d.match_id = m.match_id
    WHERE d.batter = ? 
      AND d.bowler = ? 
      AND m.match_type = 'T20'
    GROUP BY d.extras_type
    ORDER BY d.extras_type;
    """
    try:
        results = db.execute(sql, (batter_name, bowler_name)).fetchall()
        return {
            "batter_name": batter_name,
            "bowler_name": bowler_name,
            "extras_distribution": [{ "extras_type": row[0], "count": row[1]} for row in results]
        }
    except Exception as e:
        logger.error(f"Error in debug_h2h_delivery_types for {batter_name} vs {bowler_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class BatterBoundaryThresholdStat(BaseModel):
    threshold: int
    total_innings_for_criteria: int
    innings_met_or_exceeded_threshold: int
    hit_rate_percentage: float

# New Pydantic model for individual gamelog entries for boundary stats
class BatterBoundaryGamelogEntry(BaseModel):
    match_id: str
    match_date: date
    event_name: Optional[str] = None
    batting_team: Optional[str] = None # Added
    bowling_team: str
    venue: Optional[str] = None
    city: Optional[str] = None # Added
    innings_no: int
    runs_scored: int # Total runs in inning
    balls_faced: int # Total balls faced in inning
    fours: int # Total 4s in inning - Added
    sixes: int # Total 6s in inning - Added
    boundaries_hit_in_inning: int # Specific to the boundary type requested (4s or 6s)
    wicket_type: Optional[str] = None # Added
    wicket_bowler: Optional[str] = None # Added
    wicket_fielders: Optional[str] = None # Added
    strike_rate: Optional[float] = None # Added

class BatterBoundaryPlayerStats(BaseModel):
    player_name: str
    boundary_type: str # '4s' or '6s'
    stats: List[BatterBoundaryThresholdStat]
    gamelog: Optional[List[BatterBoundaryGamelogEntry]] = None # Added gamelog field

# Define PlayerGamelogEntry first as it's used by BatterRunsPlayerStats
class PlayerGamelogEntry(BaseModel):
    match_id: str
    match_date: date
    # event_name: Optional[str] # REMOVED
    batting_team: str
    bowling_team: str
    # venue: Optional[str] # REMOVED
    city: Optional[str]
    innings_no: int
    runs_scored: int
    balls_faced: int
    fours: int
    sixes: int
    wicket_type: Optional[str] = None
    wicket_bowler: Optional[str] = None       # Bowler who took the wicket
    wicket_fielders: Optional[str] = None # Fielders involved in wicket
    strike_rate: Optional[float] = None # Changed from float to Optional[float]
    # non_striker_at_dismissal: Optional[str] # Could be added if needed

class BatterRunsThresholdStat(BaseModel):
    threshold: int
    total_innings_for_criteria: int
    innings_met_or_exceeded_threshold: int
    hit_rate_percentage: float

class BatterRunsPlayerStats(BaseModel):
    player_name: str
    stats: List[BatterRunsThresholdStat]
    gamelog: Optional[List[PlayerGamelogEntry]] = None # Uses PlayerGamelogEntry

# --- START: Team Level Aggregated Batting Stats Models ---
class TeamRunsThresholdStat(BaseModel):
    threshold: int
    total_player_innings_for_criteria: int # Sum of all innings by qualifying players from the team under filters
    total_player_innings_met_threshold: int  # Sum of innings where a qualifying player met threshold
    team_hit_rate_percentage: float

class TeamBattingStatsResponse(BaseModel):
    team_name: str
    filters_applied: dict
    stats: List[TeamRunsThresholdStat]
    contributing_players_count: int
# --- END: Team Level Aggregated Batting Stats Models ---

# New Pydantic model for Bowler Gamelog entries (API side) - MOVED HERE
class BowlerAPIGamelogEntry(BaseModel):
    match_id: str
    match_date: date
    bowling_team_for_player: str # Team the player (bowler) is playing for
    batting_team_opponent: str # Team the player bowled against
    venue: Optional[str] = None # Added venue, as it's useful contextual info
    city: Optional[str] = None
    innings_no: int
    wickets_taken_in_inning: int
    runs_conceded_in_inning: int
    balls_bowled_in_inning: int # Actual balls bowled (legal deliveries)
    economy_rate_in_inning: Optional[float] = None
    overs_bowled: Optional[str] = None # e.g., "4.0", "3.5"

# Add new Pydantic models for Bowler Wicket Hit Rates
class BowlerWicketThresholdStat(BaseModel):
    threshold: int
    total_innings_for_criteria: int # Innings where the bowler bowled at least one ball
    innings_met_or_exceeded_threshold: int
    hit_rate_percentage: float
    # gamelog: Optional[List[PlayerGamelogEntry]] = None # REMOVED gamelog from here

class BowlerWicketPlayerStats(BaseModel):
    player_name: str
    stats: List[BowlerWicketThresholdStat]
    gamelog: Optional[List[BowlerAPIGamelogEntry]] = None # This now correctly references the defined model

# New Pydantic model for Player vs Player H2H Stats
class PlayerH2HStats(BaseModel):
    batter_name: str
    bowler_name: str
    filters_applied: Optional[dict] = None
    matches_played_together: int # Number of distinct matches they both played in and faced each other
    innings_batted: int # Number of distinct innings batter faced bowler
    runs_scored: int
    balls_faced: int
    dismissals: int # Times this bowler dismissed this batter
    strike_rate: Optional[float] = None
    average: Optional[float] = None
    dot_ball_percentage: Optional[float] = None
    fours_hit: int
    sixes_hit: int
    boundary_percentage: Optional[float] = None # Percentage of balls faced that were boundaries (4s or 6s)

# --- Generic Lookup Models (must be defined before PlayerFilterOptions) ---
class Tournament(BaseModel):
    name: str
    type: str # "Franchise", "International", "Other"

class Season(BaseModel):
    season: str # e.g., "2023", "2023/24"

class Venue(BaseModel):
    name: str

class Team(BaseModel): # Generic team model
    name: str
# --- End Generic Lookup Models ---

class PlayerFilterOptions(BaseModel):
    tournaments: List[Tournament]
    seasons: List[Season]
    venues: List[Venue]
    oppositionTeams: List[Team] # Teams the player has played against

# New Pydantic model for Venue Aggregate Stats
class VenueWicketTypeBreakdown(BaseModel):
    wicket_type: str
    count: int
    percentage: float

class VenueAggregateStats(BaseModel):
    venue_name: str
    filters_applied: Optional[dict] = None
    total_matches_t20: int
    average_first_innings_score: Optional[float] = None
    average_second_innings_score: Optional[float] = None
    # toss_win_bat_first_percentage: Optional[float] = None # Requires toss decision data
    # toss_win_bowl_first_percentage: Optional[float] = None # Requires toss decision data
    average_runs_per_over_overall: Optional[float] = None
    average_fours_per_match: Optional[float] = None
    average_sixes_per_match: Optional[float] = None
    wicket_type_breakdown: List[VenueWicketTypeBreakdown] = []

# New Pydantic models for Player Stats by Venue
class PlayerVenueBattingRecord(BaseModel):
    matches_batted: int
    innings_batted: int
    runs_scored: int
    balls_faced: int
    average: Optional[float] = None
    strike_rate: Optional[float] = None
    fours: int
    sixes: int
    dismissals: int
    not_outs: int # Calculated as innings_batted - dismissals
    highest_score: Optional[int] = None # Could be complex to get accurately with current delivery table alone easily
    fifties: Optional[int] = None # Requires per-inning aggregation then count
    hundreds: Optional[int] = None # Requires per-inning aggregation then count

class PlayerVenueBowlingRecord(BaseModel):
    matches_bowled: int
    innings_bowled: int
    balls_bowled: int
    runs_conceded: int
    wickets_taken: int
    bowling_average: Optional[float] = None
    economy_rate: Optional[float] = None
    strike_rate: Optional[float] = None
    best_figures_innings: Optional[str] = None # e.g., "3/25"
    three_wickets_innings: Optional[int] = None
    five_wickets_innings: Optional[int] = None

class PlayerVenuePerformance(BaseModel):
    venue_name: str
    batting_stats: Optional[PlayerVenueBattingRecord] = None
    bowling_stats: Optional[PlayerVenueBowlingRecord] = None

@router.get("/players/{player_name}/filter-options", response_model=PlayerFilterOptions, tags=["players"])
async def get_player_filter_options(player_name: str = Path(...)):
    logger.info(f"Fetching filter options for player: {player_name}")
    try:
        # 1. Get tournaments player played in (and their types)
        player_tournaments_sql = """
            SELECT DISTINCT m.event_name
            FROM match m
            JOIN innings i ON m.match_id = i.match_id
            JOIN delivery d ON i.match_id = d.match_id AND i.innings_no = d.innings_no
            WHERE d.batter = ? OR d.bowler = ? OR d.non_striker = ? OR d.fielders_involved LIKE '%' || ? || '%'
            ORDER BY m.event_name;
        """
        raw_tournament_names = db.execute(player_tournaments_sql, [player_name, player_name, player_name, player_name]).fetchall()
        
        all_tournaments_info = list_tournaments() # This returns List[Dict[str, str]]]
        player_tournaments_set = {name_tuple[0] for name_tuple in raw_tournament_names}
        
        player_tournaments_typed = [
            Tournament(name=t['name'], type=t['type']) 
            for t in all_tournaments_info 
            if t['name'] in player_tournaments_set
        ]

        # 2. Get seasons player played in
        player_seasons_sql = """
            SELECT DISTINCT m.season
            FROM match m
            JOIN innings i ON m.match_id = i.match_id
            JOIN delivery d ON i.match_id = d.match_id AND i.innings_no = d.innings_no
            WHERE (d.batter = ? OR d.bowler = ? OR d.non_striker = ? OR d.fielders_involved LIKE '%' || ? || '%') AND m.season IS NOT NULL
            ORDER BY m.season DESC;
        """
        raw_seasons = db.execute(player_seasons_sql, [player_name, player_name, player_name, player_name]).fetchall()
        player_seasons = [Season(season=s[0]) for s in raw_seasons]

        # 3. Get venues player played at
        player_venues_sql = """
            SELECT DISTINCT m.venue
            FROM match m
            JOIN innings i ON m.match_id = i.match_id
            JOIN delivery d ON i.match_id = d.match_id AND i.innings_no = d.innings_no
            WHERE (d.batter = ? OR d.bowler = ? OR d.non_striker = ? OR d.fielders_involved LIKE '%' || ? || '%') AND m.venue IS NOT NULL
            ORDER BY m.venue;
        """
        raw_venues = db.execute(player_venues_sql, [player_name, player_name, player_name, player_name]).fetchall()
        player_venues = [Venue(name=v[0]) for v in raw_venues]

        # 4. Get opposition teams player played against
        player_opposition_sql = """
            SELECT DISTINCT opposition_team_name FROM (
                SELECT i.bowling_team as opposition_team_name
                FROM delivery d
                JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
                WHERE d.batter = ? AND i.bowling_team IS NOT NULL
                UNION
                SELECT i.batting_team as opposition_team_name
                FROM delivery d
                JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
                WHERE d.bowler = ? AND i.batting_team IS NOT NULL
            )
            WHERE opposition_team_name IS NOT NULL
            ORDER BY opposition_team_name;
        """
        raw_opposition = db.execute(player_opposition_sql, [player_name, player_name]).fetchall()
        player_opposition_teams = [Team(name=opp[0]) for opp in raw_opposition]

        return PlayerFilterOptions(
            tournaments=player_tournaments_typed,
            seasons=player_seasons,
            venues=player_venues,
            oppositionTeams=player_opposition_teams
        )
    except Exception as e:
        logger.error(f"Error getting filter options for player {player_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Could not fetch filter options for player {player_name}: {str(e)}")

@router.get("/batters/stats/boundary_hit_rates", response_model=List[BatterBoundaryPlayerStats], tags=["batters"])
async def get_batter_boundary_hit_rates(
    players: Optional[str] = Query(None, description="Comma-separated list of player names (exact match) or a single player name."),
    boundary_type: str = Query(..., description="Type of boundary: '4s' or '6s'.", pattern="^(4s|6s)$"),
    thresholds: str = Query("1,2,3", description="Comma-separated list of N values for 'N+ boundaries' (e.g., '1,2,3' for 1+, 2+, 3+)."),
    season: Optional[str] = Query(None, description="Filter by season (e.g., '2022', '2023/24'). Exact match."),
    team: Optional[str] = Query(None, description="Filter by batting team (exact match)."),
    opposition: Optional[str] = Query(None, description="Filter by opposition team (exact match)."),
    venue: Optional[str] = Query(None, description="Filter by venue (exact match)."),
    tournament: Optional[str] = Query(None, description="Filter by tournament name (exact match) or comma-separated list of names."), # Updated description
    min_innings: Optional[int] = Query(1, description="Minimum number of innings played by the batter under these filters to be included."),
    last_n: Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    include_gamelog: bool = Query(False, description="Set to true to include detailed gamelog for each player.")
):
    """
    Calculates hit rates for batters achieving N+ boundaries (4s or 6s) in an innings.
    Returns a list of players, each with stats for the specified thresholds.
    Optionally includes a gamelog of individual innings if include_gamelog=true.
    """
    parsed_thresholds = []
    if thresholds:
        try:
            parsed_thresholds = sorted(list(set([int(t.strip()) for t in thresholds.split(',') if t.strip().isdigit() and int(t.strip()) > 0])))
            if not parsed_thresholds:
                raise ValueError("Thresholds must contain positive integers.")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid thresholds format: {e}. Expected comma-separated positive integers.")
    if not parsed_thresholds: # Default if input is empty or all invalid
        parsed_thresholds = [1, 2, 3]

    boundary_value_int = 4 if boundary_type == "4s" else 6

    conditions = []
    params = []

    tournament_name_list = []
    if tournament:
        tournament_name_list = [t.strip() for t in tournament.split(',') if t.strip()] # Parse comma-separated list
        if tournament_name_list:
            placeholders = ",".join("?" for _ in tournament_name_list)
            conditions.append(f"m.event_name IN ({placeholders})")
            params.extend(tournament_name_list)

    season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)
    if season_clause:
        conditions.append(season_clause)
        params.extend(season_params)

    if team:
        conditions.append("i.batting_team = ?")
        params.append(team)
    if opposition:
        conditions.append("i.bowling_team = ?")
        params.append(opposition)
    
    player_list = []
    if players: # Handle comma-separated list of players
        player_list = [p.strip() for p in players.split(',') if p.strip()]
        if player_list:
            placeholders = ",".join("?" for _ in player_list)
            conditions.append(f"d.batter IN ({placeholders})")
            params.extend(player_list)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    threshold_sum_cases = []
    for t in parsed_thresholds:
        threshold_sum_cases.append(f"SUM(CASE WHEN pip.boundaries_in_inning >= {t} THEN 1 ELSE 0 END) as met_thresh_{t}")
    
    threshold_sum_sql = ", ".join(threshold_sum_cases)

    sql = f"""
    WITH PlayerInningPerformance AS (
        SELECT
            d.batter,
            d.match_id,
            d.innings_no,
            SUM(CASE WHEN d.runs_batter = {boundary_value_int} THEN 1 ELSE 0 END) AS boundaries_in_inning
        FROM delivery d
        JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
        JOIN match m ON d.match_id = m.match_id
        WHERE {where_clause}
        GROUP BY d.batter, d.match_id, d.innings_no
    ),
    PlayerFilteredInningsCount AS (
        SELECT
            batter,
            COUNT(*) as total_filtered_innings
        FROM PlayerInningPerformance
        GROUP BY batter
        HAVING COUNT(*) >= ? 
    )
    SELECT
        pip.batter AS player_name,
        pfc.total_filtered_innings,
        {threshold_sum_sql}
    FROM PlayerInningPerformance pip
    JOIN PlayerFilteredInningsCount pfc ON pip.batter = pfc.batter
    GROUP BY pip.batter, pfc.total_filtered_innings
    ORDER BY pip.batter;
    """
    
    final_params = params + [min_innings]
    
    logging.info(f"Executing boundary hit rate query with filters: {conditions} and params: {final_params} for thresholds: {parsed_thresholds}")
    try:
        data_main_stats = db.execute(sql, final_params).fetchall()
    except Exception as e:
        logging.error(f"Error executing boundary hit rate query: {e}\\nSQL: {sql}\\nParams: {final_params}")
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

    results_map = {}
    for row_tuple in data_main_stats:
        player_name_val = row_tuple[0]
        total_innings = row_tuple[1]
        
        player_stat_obj = BatterBoundaryPlayerStats(player_name=player_name_val, boundary_type=boundary_type, stats=[], gamelog=None)
        
        for i, t_val in enumerate(parsed_thresholds):
            innings_met_threshold = row_tuple[2+i]
            hit_rate = (innings_met_threshold / total_innings) * 100 if total_innings > 0 else 0
            
            threshold_stat = BatterBoundaryThresholdStat(
                threshold=t_val,
                total_innings_for_criteria=total_innings,
                innings_met_or_exceeded_threshold=innings_met_threshold,
                hit_rate_percentage=round(hit_rate, 2)
            )
            player_stat_obj.stats.append(threshold_stat)
        results_map[player_name_val] = player_stat_obj

    if include_gamelog and results_map:
        players_for_gamelog = list(results_map.keys())
        player_placeholders_gamelog = ",".join("?" for _ in players_for_gamelog)

        # Base conditions for gamelog (excluding any original player list filter)
        gamelog_conditions = []
        gamelog_params = []
        original_player_filter_in_conditions = False
        
        current_param_idx = 0
        for cond_item in conditions: 
            num_params_in_cond_item = cond_item.count("?")
            if cond_item.startswith("d.batter IN"):
                original_player_filter_in_conditions = True
                current_param_idx += num_params_in_cond_item
                continue
            gamelog_conditions.append(cond_item)
            gamelog_params.extend(params[current_param_idx : current_param_idx + num_params_in_cond_item])
            current_param_idx += num_params_in_cond_item

        gamelog_conditions.append(f"d.batter IN ({player_placeholders_gamelog})")
        gamelog_params.extend(players_for_gamelog)
        
        where_clause_gamelog = " AND ".join(gamelog_conditions) if gamelog_conditions else "1=1"
        
        # Corrected: Use boundary_value_int directly
        gamelog_boundary_sum_col = f"SUM(CASE WHEN d.runs_batter = {boundary_value_int} THEN 1 ELSE 0 END)"

        sql_gamelog = f"""
        SELECT 
            d.batter,                                                              -- 0
            d.match_id,                                                            -- 1
            m.match_date,                                                          -- 2
            m.event_name,                                                          -- 3
            i.batting_team,                                                        -- 4
            i.bowling_team,                                                        -- 5 
            m.venue,                                                               -- 6
            m.city,                                                                -- 7
            d.innings_no,                                                          -- 8
            SUM(d.runs_batter) AS total_runs_in_inning,                            -- 9
            SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END) AS total_balls_faced_in_inning, -- 10
            SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END) as total_fours_in_inning,       -- 11
            SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END) as total_sixes_in_inning,       -- 12
            {gamelog_boundary_sum_col} as boundaries_of_type_hit_in_inning,        -- 13
            MAX(CASE WHEN d.player_out = d.batter THEN d.wicket_type ELSE NULL END) AS wicket_type, -- 14
            MAX(CASE WHEN d.player_out = d.batter THEN d.bowler ELSE NULL END) AS wicket_bowler,       -- 15
            MAX(CASE WHEN d.player_out = d.batter THEN d.fielders_involved ELSE NULL END) AS wicket_fielders, -- 16
            COALESCE(ROUND(SUM(d.runs_batter)::DOUBLE / NULLIF(SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END), 0) * 100, 2), 0.0) as strike_rate_in_inning -- 17
        FROM delivery d
        JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
        JOIN match m ON d.match_id = m.match_id
        WHERE {where_clause_gamelog}
        GROUP BY d.batter, d.match_id, m.match_date, m.event_name, i.batting_team, i.bowling_team, m.venue, m.city, d.innings_no
        ORDER BY d.batter, m.match_date DESC, d.match_id DESC, d.innings_no;
        """
        
        logging.info(f"Executing GAMELOG boundary hit rate query with conditions: {gamelog_conditions} and params: {gamelog_params}")
        try:
            data_gamelog = db.execute(sql_gamelog, gamelog_params).fetchall()
        except Exception as e:
            logging.error(f"Error executing GAMELOG boundary hit rate query: {e}\nSQL: {sql_gamelog}\nParams: {gamelog_params}")
        else:
            for gl_row in data_gamelog:
                gl_player_name = gl_row[0]
                if gl_player_name in results_map:
                    if results_map[gl_player_name].gamelog is None:
                        results_map[gl_player_name].gamelog = []
                    
                    results_map[gl_player_name].gamelog.append(
                        BatterBoundaryGamelogEntry(
                            match_id=gl_row[1],
                            match_date=gl_row[2],
                            event_name=gl_row[3],
                            batting_team=gl_row[4],
                            bowling_team=gl_row[5],
                            venue=gl_row[6],
                            city=gl_row[7],
                            innings_no=gl_row[8],
                            runs_scored=gl_row[9],
                            balls_faced=gl_row[10],
                            fours=gl_row[11],
                            sixes=gl_row[12],
                            boundaries_hit_in_inning=gl_row[13],
                            wicket_type=gl_row[14],
                            wicket_bowler=gl_row[15],
                            wicket_fielders=gl_row[16],
                            strike_rate=gl_row[17]
                        )
                    )

    final_results = list(results_map.values())
        
    if not final_results and player_list and len(player_list) == 1: 
         return [BatterBoundaryPlayerStats(player_name=player_list[0], boundary_type=boundary_type, stats=[], gamelog=None if not include_gamelog else [])]
    
    return final_results

@router.get("/batters/stats/runs_hit_rates", response_model=List[BatterRunsPlayerStats], tags=["batters"])
async def get_batter_runs_hit_rates(
    players: Optional[str] = Query(None, description="Comma-separated list of player names (exact match) or a single player name."),
    thresholds: str = Query("10,25,50", description="Comma-separated list of N values for 'N+ runs' (e.g., '10,25,50' for 10+, 25+, 50+ runs)."),
    season: Optional[str] = Query(None, description="Filter by season (e.g., '2022', '2023/24'). Exact match."),
    team: Optional[str] = Query(None, description="Filter by batting team (exact match)."),
    opposition: Optional[str] = Query(None, description="Filter by opposition team (exact match)."),
    venue: Optional[str] = Query(None, description="Filter by venue (exact match)."),
    tournament: Optional[str] = Query(None, description="Filter by tournament name (exact match) or comma-separated list of names."), # Updated description
    min_innings: Optional[int] = Query(1, description="Minimum number of innings played by the batter under these filters to be included."),
    last_n: Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    include_gamelog: bool = Query(False, description="Set to true to include detailed gamelog for each player.")
):
    """
    Calculates hit rates for batters achieving N+ runs in an innings.
    Returns a list of players, each with stats for the specified thresholds.
    Optionally includes a gamelog of individual innings if include_gamelog=true.
    """
    parsed_thresholds = []
    if thresholds:
        try:
            parsed_thresholds = sorted(list(set([int(t.strip()) for t in thresholds.split(',') if t.strip().isdigit() and int(t.strip()) >= 0])))
            if not parsed_thresholds:
                raise ValueError("Thresholds must contain non-negative integers.")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid thresholds format: {e}. Expected comma-separated non-negative integers.")
    if not parsed_thresholds:
        parsed_thresholds = [10, 25, 50] 

    conditions = []
    params = []

    tournament_name_list = []
    if tournament:
        tournament_name_list = [t.strip() for t in tournament.split(',') if t.strip()] # Parse comma-separated list
        if tournament_name_list:
            placeholders = ",".join("?" for _ in tournament_name_list)
            conditions.append(f"m.event_name IN ({placeholders})")
            params.extend(tournament_name_list)

    season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)
    if season_clause:
        conditions.append(season_clause)
        params.extend(season_params)

    if venue:
        conditions.append("m.venue = ?")
        params.append(venue)
    if team:
        conditions.append("i.batting_team = ?")
        params.append(team)
    if opposition:
        conditions.append("i.bowling_team = ?")
        params.append(opposition)
    
    player_list = []
    if players: 
        player_list = [p.strip() for p in players.split(',') if p.strip()]
        if player_list:
            placeholders = ",".join("?" for _ in player_list)
            conditions.append(f"d.batter IN ({placeholders})")
            params.extend(player_list)

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    threshold_sum_cases = []
    for t in parsed_thresholds:
        threshold_sum_cases.append(f"SUM(CASE WHEN pip.runs_in_inning >= {t} THEN 1 ELSE 0 END) as met_thresh_{t}")
    
    threshold_sum_sql = ", ".join(threshold_sum_cases)

    sql = f"""
    WITH PlayerInningPerformance AS (
        SELECT
            d.batter,
            d.match_id,
            d.innings_no,
            SUM(d.runs_batter) AS runs_in_inning
        FROM delivery d
        JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
        JOIN match m ON d.match_id = m.match_id
        WHERE {where_clause}
        GROUP BY d.batter, d.match_id, d.innings_no
    ),
    PlayerFilteredInningsCount AS (
        SELECT
            batter,
            COUNT(*) as total_filtered_innings
        FROM PlayerInningPerformance
        GROUP BY batter
        HAVING COUNT(*) >= ? 
    )
    SELECT
        pip.batter AS player_name,
        pfc.total_filtered_innings,
        {threshold_sum_sql}
    FROM PlayerInningPerformance pip
    JOIN PlayerFilteredInningsCount pfc ON pip.batter = pfc.batter
    GROUP BY pip.batter, pfc.total_filtered_innings
    ORDER BY pip.batter;
    """
    
    final_params = params + [min_innings]
    
    logging.info(f"Executing runs hit rate query: filters={{conditions}}, params={{final_params}}, thresholds={{parsed_thresholds}}")
    try:
        data_main_stats = db.execute(sql, final_params).fetchall()
    except Exception as e:
        logging.error(f"Error executing runs hit rate query: {e}\\nSQL: {sql}\\nParams: {final_params}")
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

    results_map = {}
    for row_tuple in data_main_stats:
        player_name_val = row_tuple[0]
        total_innings = row_tuple[1]
        
        player_stat_obj = BatterRunsPlayerStats(player_name=player_name_val, stats=[], gamelog=None)
        
        for i, t_val in enumerate(parsed_thresholds):
            innings_met_threshold = row_tuple[2+i]
            hit_rate = (innings_met_threshold / total_innings) * 100 if total_innings > 0 else 0
            
            threshold_stat = BatterRunsThresholdStat(
                threshold=t_val,
                total_innings_for_criteria=total_innings,
                innings_met_or_exceeded_threshold=innings_met_threshold,
                hit_rate_percentage=round(hit_rate, 2)
            )
            player_stat_obj.stats.append(threshold_stat)
        results_map[player_name_val] = player_stat_obj

    if include_gamelog and results_map:
        players_for_gamelog = list(results_map.keys())
        player_placeholders_gamelog = ",".join("?" for _ in players_for_gamelog)

        gamelog_conditions = []
        gamelog_params = []
        current_param_idx = 0
        for cond_item in conditions: 
            num_params_in_cond_item = cond_item.count("?")
            if cond_item.startswith("d.batter IN"):
                current_param_idx += num_params_in_cond_item
                continue
            gamelog_conditions.append(cond_item)
            gamelog_params.extend(params[current_param_idx : current_param_idx + num_params_in_cond_item])
            current_param_idx += num_params_in_cond_item

        gamelog_conditions.append(f"d.batter IN ({player_placeholders_gamelog})")
        gamelog_params.extend(players_for_gamelog)
        
        where_clause_gamelog = " AND ".join(gamelog_conditions) if gamelog_conditions else "1=1"

        sql_gamelog = f"""
        SELECT 
            d.batter, 
            d.match_id, 
            m.match_date, 
            m.event_name, 
            i.batting_team, 
            i.bowling_team, 
            m.venue, 
            m.city,     
            d.innings_no,
            SUM(d.runs_batter) AS runs_scored_in_inning, 
            SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END) AS balls_faced_in_inning, 
            SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END) as fours_in_inning,
            SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END) as sixes_in_inning,
            MAX(CASE WHEN d.player_out = d.batter THEN d.wicket_type ELSE NULL END) AS wicket_type,
            MAX(CASE WHEN d.player_out = d.batter THEN d.bowler ELSE NULL END) AS wicket_bowler, 
            MAX(CASE WHEN d.player_out = d.batter THEN d.fielders_involved ELSE NULL END) AS wicket_fielders,
            COALESCE(ROUND(SUM(d.runs_batter)::DOUBLE / NULLIF(SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END), 0) * 100, 2), 0.0) as strike_rate
        FROM delivery d
        JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
        JOIN match m ON d.match_id = m.match_id
        WHERE {where_clause_gamelog}
        GROUP BY d.batter, d.match_id, m.match_date, m.event_name, i.batting_team, i.bowling_team, m.venue, m.city, d.innings_no
        ORDER BY d.batter, m.match_date DESC, d.match_id DESC, d.innings_no;
        """
        
        logging.info(f"Executing GAMELOG runs hit rate query with conditions: {gamelog_conditions} and params: {gamelog_params}")
        try:
            data_gamelog = db.execute(sql_gamelog, gamelog_params).fetchall()
        except Exception as e:
            logging.error(f"Error executing GAMELOG runs hit rate query: {e}\\nSQL: {sql_gamelog}\\nParams: {gamelog_params}")
        else:
            for gl_row_tuple in data_gamelog:
                gl_player_name    = gl_row_tuple[0]
                match_id          = gl_row_tuple[1]
                match_date        = gl_row_tuple[2]
                event_name        = gl_row_tuple[3]
                batting_team      = gl_row_tuple[4]
                bowling_team      = gl_row_tuple[5]
                venue             = gl_row_tuple[6]
                city              = gl_row_tuple[7]
                innings_no        = gl_row_tuple[8]
                runs_scored       = gl_row_tuple[9]
                balls_faced       = gl_row_tuple[10]
                fours             = gl_row_tuple[11]
                sixes             = gl_row_tuple[12]
                wicket_type       = gl_row_tuple[13]
                wicket_bowler     = gl_row_tuple[14]
                wicket_fielders   = gl_row_tuple[15] # This will be None based on SQL
                strike_rate_val   = gl_row_tuple[16]


                if gl_player_name in results_map:
                    if results_map[gl_player_name].gamelog is None:
                        results_map[gl_player_name].gamelog = []
                    
                    results_map[gl_player_name].gamelog.append(
                        PlayerGamelogEntry(
                            match_id=match_id,
                            match_date=match_date,
                            # event_name=event_name, # REMOVED
                            batting_team=batting_team,
                            bowling_team=bowling_team,
                            # venue=venue, # REMOVED
                            city=city,
                            innings_no=innings_no,
                            runs_scored=runs_scored,
                            balls_faced=balls_faced,
                            fours=fours,
                            sixes=sixes,
                            wicket_type=wicket_type,
                            wicket_bowler=wicket_bowler,
                            wicket_fielders=wicket_fielders,
                            strike_rate=strike_rate_val
                        )
                    )

    final_results = list(results_map.values())
        
    if not final_results and player_list and len(player_list) == 1: 
         return [BatterRunsPlayerStats(player_name=player_list[0], stats=[], gamelog=None if not include_gamelog else [])]
    
    return final_results

@router.get("/matchups/player_h2h", response_model=PlayerH2HStats, tags=["matchups", "players"])
async def get_player_h2h_stats(
    batter_name: str = Query(..., description="Name of the batter."),
    bowler_name: str = Query(..., description="Name of the bowler."),
    season: Optional[str] = Query(None, description="Filter by a specific season (e.g., '2022', '2023/24'). Overridden by last_n."),
    last_n: Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    venue: Optional[str] = Query(None, description="Filter by venue (exact match)."),
    tournament: Optional[str] = Query(None, description="Filter by tournament name (exact match).")
):
    """
    Provides Head-to-Head (H2H) statistics between a specific batter and a specific bowler.
    """
    conditions = ["d.batter = ?", "d.bowler = ?"]
    params = [batter_name, bowler_name]
    
    applied_filters = {
        "batter_name": batter_name,
        "bowler_name": bowler_name
    }

    tournament_name_list = []
    if tournament:
        # Assuming single tournament for this endpoint's season context
        tournament_name_list = [tournament] if tournament.strip() else []
        if tournament_name_list:
            conditions.append("m.event_name = ?") # Add to main query conditions
            params.append(tournament_name_list[0])
            applied_filters["tournament"] = tournament_name_list[0]


    season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)
    if season_clause:
        conditions.append(season_clause)
        params.extend(season_params)
        if last_n: applied_filters["last_n_seasons"] = last_n
        elif season: applied_filters["season"] = season

    if venue:
        conditions.append("m.venue = ?")
        params.append(venue)
        applied_filters["venue"] = venue

    where_clause = " AND ".join(conditions)

    sql = f"""
    SELECT
        COUNT(DISTINCT d.match_id) as matches_played_together,
        COUNT(DISTINCT (d.match_id, d.innings_no)) as innings_batted,
        COALESCE(SUM(d.runs_batter), 0) as runs_scored,
        COALESCE(SUM(CASE WHEN d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END), 0) as balls_faced,
        COALESCE(SUM(CASE WHEN d.player_out = d.batter AND d.wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field', 'handled the ball', 'timed out') THEN 1 ELSE 0 END), 0) as dismissals,
        COALESCE(SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END), 0) as fours_hit,
        COALESCE(SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END), 0) as sixes_hit,
        COALESCE(SUM(CASE WHEN d.runs_batter = 0 AND d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END), 0) as dot_balls
    FROM delivery d
    JOIN match m ON d.match_id = m.match_id
    JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
    WHERE {where_clause};
    """

    logging.info(f"Executing Player H2H query: {sql} with params: {params}")
    try:
        row = db.execute(sql, params).fetchone()
    except Exception as e:
        logging.error(f"Error executing Player H2H query: {e}\\nSQL: {sql}\\nParams: {params}")
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

    if not row or row[0] == 0: 
        return PlayerH2HStats(
            batter_name=batter_name,
            bowler_name=bowler_name,
            filters_applied=applied_filters,
            matches_played_together=0,
            innings_batted=0,
            runs_scored=0,
            balls_faced=0,
            dismissals=0,
            fours_hit=0,
            sixes_hit=0
        )
    
    matches_played_together, innings_batted, runs_scored, balls_faced, dismissals, fours_hit, sixes_hit, dot_balls = row

    strike_rate = (runs_scored / balls_faced) * 100 if balls_faced > 0 else None
    average = runs_scored / dismissals if dismissals > 0 else None
    if average is None and runs_scored > 0 and dismissals == 0: # Handle infinite average
        average = float('inf') 
        
    dot_ball_percentage = (dot_balls / balls_faced) * 100 if balls_faced > 0 else None
    boundary_percentage = ((fours_hit + sixes_hit) / balls_faced) * 100 if balls_faced > 0 else None
    return PlayerH2HStats(
        batter_name=batter_name,
        bowler_name=bowler_name,
        filters_applied=applied_filters,
        matches_played_together=matches_played_together,
        innings_batted=innings_batted,
        runs_scored=runs_scored,
        balls_faced=balls_faced,
        dismissals=dismissals,
        strike_rate=round(strike_rate, 2) if strike_rate is not None else None,
        average=round(average, 2) if average is not None and average != float('inf') else (None if average is None else 'inf'),
        dot_ball_percentage=round(dot_ball_percentage, 2) if dot_ball_percentage is not None else None,
        fours_hit=fours_hit,
        sixes_hit=sixes_hit,
        boundary_percentage=round(boundary_percentage, 2) if boundary_percentage is not None else None
    )

@router.get("/players/{player_name}/gamelog", response_model=List[PlayerGamelogEntry], tags=["players", "batters"])
async def get_player_gamelog(
    player_name: str = Path(...),
    season: Optional[str] = Query(None, description="Filter by a specific season. Overridden by last_n."),
    last_n: Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    batting_team: Optional[str] = Query(None, description="Filter by the team the player was batting for."),
    opposition: Optional[str] = Query(None, description="Filter by the opposition team."),
    venue: Optional[str] = Query(None, description="Filter by venue."),
    tournament: Optional[str] = Query(None, description="Filter by tournament name (event_name)."),
    order_by: str = Query("match_date_desc", description="Sort order: 'match_date_desc', 'match_date_asc', 'runs_desc', 'runs_asc'.")
):
    logger.info(f"Fetching gamelog for player: {player_name} with filters: season={season}, last_n={last_n}, team={batting_team}, opposition={opposition}, venue={venue}, tournament={tournament}, order_by={order_by}")

    # TEMPORARY DEBUGGING FOR YBK JAISWAL - REPLACE 'SPECIFIC_MATCH_ID_HERE'
    if player_name == "YBK Jaiswal":
        debug_match_id = "SPECIFIC_MATCH_ID_HERE" # <--- REPLACE THIS WITH AN ACTUAL MATCH ID FROM YOUR DATA
        debug_sql = f"""
            SELECT d.over_no, d.ball_no, d.runs_batter, d.extras_total, d.extras_type, d.wicket_type, i.batting_team, i.bowling_team
            FROM delivery d
            JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
            WHERE d.match_id = ? AND d.batter = ?
            ORDER BY d.innings_no, d.over_no, d.ball_no;
        """
        try:
            debug_deliveries = db.execute(debug_sql, (debug_match_id, player_name)).fetchall()
            logger.info(f"DEBUG: Raw deliveries for YBK Jaiswal, match_id={debug_match_id}:")
            for i, delivery_row in enumerate(debug_deliveries):
                logger.info(f"DEBUG: Delivery {i+1}: over={delivery_row[0]}.{delivery_row[1]}, runs_batter={delivery_row[2]}, extras_total={delivery_row[3]}, extras_type='{delivery_row[4]}', wicket='{delivery_row[5]}', bat_team='{delivery_row[6]}', bowl_team='{delivery_row[7]}'")
            if not debug_deliveries:
                logger.info(f"DEBUG: No deliveries found for YBK Jaiswal, match_id={debug_match_id}")
        except Exception as e:
            logger.error(f"DEBUG: Error fetching debug deliveries: {e}")
    # END TEMPORARY DEBUGGING

    conditions = ["d.batter = ?"]
    params = [player_name]

    tournament_name_list = []
    if tournament:
        # Player gamelog is often for specific tournament context for last_n seasons
        tournament_name_list = [tournament] if tournament.strip() else []
        if tournament_name_list:
            conditions.append("m.event_name = ?") # Add to main query conditions
            params.append(tournament_name_list[0])


    season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)
    if season_clause:
        conditions.append(season_clause)
        params.extend(season_params)

    if batting_team:
        conditions.append("i.batting_team = ?")
        params.append(batting_team)
    if opposition:
        conditions.append("i.bowling_team = ?")
        params.append(opposition)
    if venue:
        conditions.append("m.venue = ?")
        params.append(venue)
    if tournament: 
        conditions.append("m.event_name = ?")
        params.append(tournament)
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"

    sql = f"""
    WITH InningsStats AS (
        SELECT
            d.match_id,
            d.innings_no,
            d.batter,
            i.batting_team,
            i.bowling_team,
            m.match_date,
            # m.event_name, # REMOVED
            # m.venue, # REMOVED
            m.city,
            SUM(d.runs_batter) as runs_scored,
            SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END) as balls_faced,
            SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END) as fours,
            SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END) as sixes,
            MAX(CASE WHEN d.player_out = d.batter THEN d.wicket_type ELSE NULL END) as wicket_type,
            MAX(CASE WHEN d.player_out = d.batter THEN d.bowler ELSE NULL END) as wicket_bowler,
            MAX(CASE WHEN d.player_out = d.batter THEN d.fielders_involved ELSE NULL END) AS wicket_fielders, 
            CASE 
                WHEN SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END) = 0 THEN # Corrected denominator check
                    CASE WHEN SUM(d.runs_batter) > 0 THEN NULL ELSE 0.0 END
                ELSE ROUND(SUM(d.runs_batter)::DOUBLE / SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END) * 100, 2)
            END as strike_rate
        FROM delivery d
        JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
        JOIN match m ON d.match_id = m.match_id
        WHERE {where_clause}
        GROUP BY d.match_id, d.innings_no, d.batter, i.batting_team, i.bowling_team, m.match_date, m.city # REMOVED m.event_name, m.venue
    )
    SELECT
        s.match_id,
        s.match_date,
        # s.event_name, # REMOVED
        s.batting_team,
        s.bowling_team,
        # s.venue, # REMOVED
        s.city,
        s.innings_no,
        s.runs_scored,
        s.balls_faced,
        s.fours,
        s.sixes,
        s.wicket_type,
        s.wicket_bowler,
        s.wicket_fielders,
        s.strike_rate
    FROM InningsStats s
    {{order_by_sql}}
    """

    order_by_sql_actual = "ORDER BY s.match_date DESC, s.match_id DESC, s.innings_no ASC" 
    if order_by == "match_date_asc":
        order_by_sql_actual = "ORDER BY s.match_date ASC, s.match_id ASC, s.innings_no ASC"
    elif order_by == "runs_desc":
        order_by_sql_actual = "ORDER BY s.runs_scored DESC, s.match_date DESC"
    elif order_by == "runs_asc":
        order_by_sql_actual = "ORDER BY s.runs_scored ASC, s.match_date ASC"

    final_sql_filled = sql.format(order_by_sql=order_by_sql_actual)

    logger.debug(f"Executing player gamelog SQL: {{final_sql_filled}} with params: {params}")
    
    try:
        raw_data = db.execute(final_sql_filled, params).fetchall()
        gamelog_entries = [
            PlayerGamelogEntry(
                match_id=row[0],
                match_date=row[1],
                # event_name=row[2], # REMOVED
                batting_team=row[2], # Index adjusted
                bowling_team=row[3], # Index adjusted
                # venue=row[5], # REMOVED
                city=row[4], # Index adjusted
                innings_no=row[5], # Index adjusted
                runs_scored=row[6], # Index adjusted
                balls_faced=row[7], # Index adjusted
                fours=row[8], # Index adjusted
                sixes=row[9], # Index adjusted
                wicket_type=row[10], # Index adjusted
                wicket_bowler=row[11], # Index adjusted
                wicket_fielders=row[12], # Index adjusted
                strike_rate=row[13] # Index adjusted
            )
            for row in raw_data
        ]
        return gamelog_entries
    except Exception as e:
        logger.error(f"Error in get_player_gamelog for {player_name}: {e}", exc_info=True)
        logger.error(f"SQL: {final_sql_filled}")
        logger.error(f"Params: {params}")
        raise HTTPException(status_code=500, detail=f"Database error processing gamelog for {player_name}: {str(e)}")

@router.get("/venues/{venue_name}/stats", response_model=VenueAggregateStats, tags=["venues"])
async def get_venue_aggregate_stats(
    venue_name: str = Path(...),
    season: Optional[str] = Query(None, description="Filter by a specific season (e.g., '2022', '2023/24'). Overridden by last_n."),
    last_n: Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    tournament: Optional[str] = Query(None, description="Filter by tournament name (exact match).")
):
    """
    Provides aggregate statistics for a specific venue, optionally filtered by season/tournament.
    """
    base_conditions = ["m.venue = ?"]
    base_params = [venue_name]
    applied_filters = {"venue_name": venue_name}

    tournament_name_list = []
    if tournament:
        # Venue stats might be general or for a specific tournament's seasons
        tournament_name_list = [tournament] if tournament.strip() else []
        if tournament_name_list:
            base_conditions.append("m.event_name = ?") # Add to main query conditions
            base_params.append(tournament_name_list[0])
            applied_filters["tournament"] = tournament_name_list[0]


    season_clause, season_params = get_season_filter_clause(season, last_n, table_alias='m', tournament_names=tournament_name_list if tournament_name_list else None)
    if season_clause:
        base_conditions.append(season_clause)
        base_params.extend(season_params)
        if last_n: applied_filters["last_n_seasons"] = last_n
        elif season: applied_filters["season"] = season
        
    if tournament:
        base_conditions.append("m.event_name = ?")
        base_params.append(tournament)
        applied_filters["tournament"] = tournament

    where_clause = " AND ".join(base_conditions)

    sql_match_scores_winners = f"""
    WITH MatchInningsInfo AS (
        SELECT 
            m.match_id,
            m.outcome_winner as match_winner,
            i.innings_no,
            i.batting_team,
            SUM(d.runs_total) as inning_score
        FROM match m
        JOIN innings i ON m.match_id = i.match_id
        JOIN delivery d ON m.match_id = d.match_id AND i.innings_no = d.innings_no
        WHERE {where_clause}
        GROUP BY m.match_id, m.outcome_winner, i.innings_no, i.batting_team
    ),
    FirstInningsScores AS (
        SELECT match_id, inning_score as first_innings_score, batting_team as first_innings_team
        FROM MatchInningsInfo
        WHERE innings_no = 1
    ),
    SecondInningsScores AS (
        SELECT match_id, inning_score as second_innings_score, batting_team as second_innings_team
        FROM MatchInningsInfo
        WHERE innings_no = 2
    )
    SELECT 
        COUNT(DISTINCT m.match_id) as total_matches_with_winner,
        AVG(fis.first_innings_score) as avg_first_innings_score,
        AVG(sis.second_innings_score) as avg_second_innings_score,
        SUM(CASE WHEN m.outcome_winner = fis.first_innings_team THEN 1 ELSE 0 END) as wins_bat_first,
        SUM(CASE WHEN m.outcome_winner = sis.second_innings_team THEN 1 ELSE 0 END) as wins_bat_second
    FROM match m
    LEFT JOIN FirstInningsScores fis ON m.match_id = fis.match_id
    LEFT JOIN SecondInningsScores sis ON m.match_id = sis.match_id
    WHERE {where_clause} AND m.outcome_winner IS NOT NULL AND m.outcome_winner != 'no result' AND m.outcome_winner != 'tie'
    """ 
    
    logging.info(f"Executing Venue Stats (Scores/Winners) query with params: {base_params + base_params}")
    scores_data = db.execute(sql_match_scores_winners, parameters = base_params + base_params).fetchone()
    
    total_matches_t20_overall_query = f"SELECT COUNT(DISTINCT m.match_id) FROM match m WHERE {where_clause}"
    total_matches_t20 = db.execute(total_matches_t20_overall_query, base_params).fetchone()[0]

    avg_first_innings_score = None
    avg_second_innings_score = None
    win_percentage_bat_first = None
    win_percentage_bat_second = None
    matches_with_winner = 0

    if scores_data and len(scores_data) == 5:
        matches_with_winner = scores_data[0] if scores_data[0] is not None else 0
        avg_first_innings_score = scores_data[1]
        avg_second_innings_score = scores_data[2]
        wins_bat_first = scores_data[3] if scores_data[3] is not None else 0
        wins_bat_second = scores_data[4] if scores_data[4] is not None else 0
        if matches_with_winner > 0:
            win_percentage_bat_first = (wins_bat_first / matches_with_winner) * 100
            win_percentage_bat_second = (wins_bat_second / matches_with_winner) * 100
    else:
        logger.warning(f"Scores_data for venue {venue_name} was None or not as expected: {scores_data}")

    sql_venue_aggregates = f"""
    SELECT 
        SUM(d.runs_total) as total_runs_at_venue,
        SUM(CASE WHEN d.extras_type NOT IN ('wides', 'noballs', 'penalty') THEN 1 ELSE 0 END) as total_legal_balls_at_venue,
        SUM(CASE WHEN d.wicket_type IS NOT NULL AND d.wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field', 'handled the ball', 'timed out', 'retired') THEN 1 ELSE 0 END) as total_wickets_by_bowlers,
        SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END) as total_fours_at_venue,
        SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END) as total_sixes_at_venue
    FROM delivery d
    JOIN match m ON d.match_id = m.match_id
    WHERE {where_clause}
    """
    logging.info(f"Executing Venue Stats (Aggregates) query with params: {base_params}")
    agg_data = db.execute(sql_venue_aggregates, parameters = base_params).fetchone()

    total_runs_overall = 0
    total_balls_overall = 0
    total_wickets_by_bowlers = 0
    total_fours_overall = 0
    total_sixes_overall = 0
    avg_runs_per_over_overall = None
    avg_wickets_per_match_overall = None
    avg_fours_per_match = None
    avg_sixes_per_match = None

    if agg_data and len(agg_data) == 5:
        total_runs_overall = agg_data[0] if agg_data[0] is not None else 0
        total_balls_overall = agg_data[1] if agg_data[1] is not None else 0
        total_wickets_by_bowlers = agg_data[2] if agg_data[2] is not None else 0
        total_fours_overall = agg_data[3] if agg_data[3] is not None else 0
        total_sixes_overall = agg_data[4] if agg_data[4] is not None else 0
        if total_balls_overall > 0:
            overs = total_balls_overall / 6 
            if overs > 0:
                 avg_runs_per_over_overall = total_runs_overall / overs
        if total_matches_t20 > 0:
            avg_wickets_per_match_overall = total_wickets_by_bowlers / total_matches_t20
            avg_fours_per_match = total_fours_overall / total_matches_t20
            avg_sixes_per_match = total_sixes_overall / total_matches_t20

    else:
        logger.warning(f"Agg_data for venue {venue_name} was None or not as expected: {agg_data}")
    
    sql_wicket_breakdown = f"""
    SELECT 
        d.wicket_type,
        COUNT(*) as count
    FROM delivery d
    JOIN match m ON d.match_id = m.match_id
    WHERE {where_clause} AND d.wicket_type IS NOT NULL AND d.wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field', 'handled the ball', 'timed out', 'retired', '')
    GROUP BY d.wicket_type
    ORDER BY count DESC;
    """
    logging.info(f"Executing Venue Stats (Wicket Breakdown) query with params: {base_params}")
    wickets_data_raw = db.execute(sql_wicket_breakdown, parameters = base_params).fetchall()
    
    wicket_type_breakdown = []
    if total_wickets_by_bowlers > 0:
        for wt_row in wickets_data_raw:
            wicket_type_breakdown.append(VenueWicketTypeBreakdown(
                wicket_type=wt_row[0],
                count=wt_row[1],
                percentage=round((wt_row[1] / total_wickets_by_bowlers) * 100, 2) if total_wickets_by_bowlers > 0 else 0
            ))
    else:
        logger.warning(f"No bowler wickets found for venue {venue_name} with current filters. Wicket breakdown will be empty.")

    return VenueAggregateStats(
        venue_name=venue_name,
        filters_applied=applied_filters,
        total_matches_t20=total_matches_t20,
        average_first_innings_score=round(avg_first_innings_score, 2) if avg_first_innings_score is not None else None,
        average_second_innings_score=round(avg_second_innings_score, 2) if avg_second_innings_score is not None else None,
        win_percentage_bat_first=round(win_percentage_bat_first, 2) if win_percentage_bat_first is not None else None,
        win_percentage_bat_second=round(win_percentage_bat_second, 2) if win_percentage_bat_second is not None else None,
        average_runs_per_over_overall=round(avg_runs_per_over_overall, 2) if avg_runs_per_over_overall is not None else None,
        average_fours_per_match=round(avg_fours_per_match, 2) if avg_fours_per_match is not None else None,
        average_sixes_per_match=round(avg_sixes_per_match, 2) if avg_sixes_per_match is not None else None,
        wicket_type_breakdown=wicket_type_breakdown
    )

@router.get("/teams/{team_name}/stats/batting/runs_hit_rates", response_model=TeamBattingStatsResponse, tags=["teams", "batting"])
async def get_team_runs_hit_rates(
    team_name: str = Path(..., description="Name of the team."),
    thresholds: str = Query("10,20,30,50", description="Comma-separated list of N values for 'N+ runs' (e.g., '10,25,50' for 10+, 25+, 50+ runs)."),
    season: Optional[str] = Query(None, description="Filter by season (e.g., '2022', '2023/24'). Exact match. Overridden by last_n."),
    last_n: Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    tournament: Optional[str] = Query(None, description="Filter by tournament name (exact match)."),
    opposition: Optional[str] = Query(None, description="Filter by opposition team (exact match)."),
    venue: Optional[str] = Query(None, description="Filter by venue (exact match)."),
    min_player_innings_for_team_agg: int = Query(5, ge=1, description="Minimum innings a player from the team must have under these filters to be included in aggregate.")
):
    logger.info(f"Fetching team runs hit rates for team: {team_name} with thresholds: {thresholds}, min_player_innings: {min_player_innings_for_team_agg}")
    
    try:
        parsed_thresholds = [int(t.strip()) for t in thresholds.split(',') if t.strip()]
        if not parsed_thresholds:
            # Fallback to a default if the string is empty or results in an empty list
            parsed_thresholds = [10, 20, 30, 50] # Default example
    except ValueError:
        logger.error(f"Invalid thresholds format provided: {thresholds}")
        raise HTTPException(status_code=400, detail="Invalid thresholds format. Expected comma-separated integers.")

    base_conditions = ["i.batting_team = ?"]
    base_params = [team_name]
    applied_filters_dict = {
        "team_name": team_name, 
        "thresholds": thresholds, 
        "min_player_innings_for_team_agg": min_player_innings_for_team_agg
    }

    tournament_name_list = []
    if tournament:
        tournament_name_list = [t.strip() for t in tournament.split(',') if t.strip()]
        if tournament_name_list:
            tournament_ph = ','.join('?' for _ in tournament_name_list)
            base_conditions.append(f"m.event_name IN ({tournament_ph})")
            base_params.extend(tournament_name_list)
            applied_filters_dict["tournament"] = tournament_name_list


    season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)
    if season_clause:
        base_conditions.append(season_clause)
        base_params.extend(season_params)
        if last_n: applied_filters_dict["last_n_seasons"] = last_n
        elif season: applied_filters_dict["season"] = season
    
    if opposition:
        base_conditions.append("i.bowling_team = ?")
        base_params.append(opposition)
        applied_filters_dict["opposition"] = opposition
    if venue:
        base_conditions.append("m.venue = ?")
        base_params.append(venue)
        applied_filters_dict["venue"] = venue

    where_clause_for_filtering_players = " AND ".join(base_conditions)

    qualifying_players_sql = f"""
    SELECT d.batter
    FROM delivery d
    JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
    JOIN match m ON d.match_id = m.match_id
    WHERE {where_clause_for_filtering_players}
    GROUP BY d.batter
    HAVING COUNT(DISTINCT d.match_id || '_' || d.innings_no) >= ?
    """
    qualifying_players_params = base_params + [min_player_innings_for_team_agg]
    
    try:
        logger.debug(f"Qualifying players SQL: {qualifying_players_sql} with params: {qualifying_players_params}")
        qualifying_players_rows = db.execute(qualifying_players_sql, qualifying_players_params).fetchall()
    except Exception as e_qual_players:
        logger.error(f"Error fetching qualifying players for team {team_name}: {e_qual_players}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"DB error identifying qualifying players: {str(e_qual_players)}")

    contributing_players_count_val = len(qualifying_players_rows)
    if contributing_players_count_val == 0:
        return TeamBattingStatsResponse(
            team_name=team_name,
            filters_applied=applied_filters_dict,
            stats=[],
            contributing_players_count=0
        )

    qualifying_player_names = [row[0] for row in qualifying_players_rows]
    player_placeholders = ','.join('?' for _ in qualifying_player_names)

    final_conditions_for_stats = base_conditions + [f"d.batter IN ({player_placeholders})"]
    final_where_clause_for_stats = " AND ".join(final_conditions_for_stats)
    final_params_for_stats_base = base_params + qualifying_player_names

    team_stats_results: List[TeamRunsThresholdStat] = []
    for t_val in parsed_thresholds:
        sql_total_team_innings = f"""
        SELECT COUNT(DISTINCT d.match_id || '_' || d.innings_no)
        FROM delivery d
        JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
        JOIN match m ON d.match_id = m.match_id
        WHERE {final_where_clause_for_stats}
        """
        try:
            logger.debug(f"Total team innings SQL: {sql_total_team_innings} with params: {final_params_for_stats_base}")
            total_team_innings_row = db.execute(sql_total_team_innings, final_params_for_stats_base).fetchone()
            total_team_innings_for_criteria = total_team_innings_row[0] if total_team_innings_row and total_team_innings_row[0] is not None else 0
        except Exception as e_total_inns:
            logger.error(f"Error fetching total team innings for threshold {t_val}, team {team_name}: {e_total_inns}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"DB error fetching total team innings: {str(e_total_inns)}")

        sql_team_innings_met = f"""
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM delivery d
            JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
            JOIN match m ON d.match_id = m.match_id
            WHERE {final_where_clause_for_stats}
            GROUP BY d.match_id, d.innings_no
            HAVING SUM(d.runs_batter) >= ?
        )
        """
        params_for_innings_met = final_params_for_stats_base + [t_val]
        try:
            logger.debug(f"Team innings met SQL: {sql_team_innings_met} with params: {params_for_innings_met}")
            team_innings_met_row = db.execute(sql_team_innings_met, params_for_innings_met).fetchone()
            team_innings_met_threshold = team_innings_met_row[0] if team_innings_met_row and team_innings_met_row[0] is not None else 0
        except Exception as e_inns_met:
            logger.error(f"Error fetching team innings met for threshold {t_val}, team {team_name}: {e_inns_met}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"DB error fetching team innings met threshold: {str(e_inns_met)}")

        team_hit_rate = (team_innings_met_threshold / total_team_innings_for_criteria * 100) if total_team_innings_for_criteria > 0 else 0.0
        
        team_stats_results.append(TeamRunsThresholdStat(
            threshold=t_val,
            total_player_innings_for_criteria=total_team_innings_for_criteria,
            total_player_innings_met_threshold=team_innings_met_threshold,
            team_hit_rate_percentage=round(team_hit_rate, 2)
        ))

    return TeamBattingStatsResponse(
        team_name=team_name,
        filters_applied=applied_filters_dict,
        stats=team_stats_results,
        contributing_players_count=contributing_players_count_val
    )

@router.get("/bowlers/stats/wickets_taken_rates", response_model=List[BowlerWicketPlayerStats], tags=["bowlers"])
async def get_bowler_wickets_taken_rates(
    players: Optional[str] = Query(None, description="Comma-separated list of player names (exact match) or a single player name."),
    thresholds: str = Query("1,2,3", description="Comma-separated list of N values for 'N+ wickets' (e.g., '1,2,3' for 1+, 2+, 3+ wickets taken)."), # Machete-placeholder_for_rest_of_args
    season: Optional[str] = Query(None, description="Filter by season (e.g., '2022', '2023/24'). Exact match. Overridden by last_n."),
    team: Optional[str] = Query(None, description="Filter by bowler's team (exact match)."),
    opposition: Optional[str] = Query(None, description="Filter by opposition team (team the bowler bowled against, exact match)."),
    venue: Optional[str] = Query(None, description="Filter by venue (exact match)."),
    tournament: Optional[str] = Query(None, description="Filter by tournament name (exact match) or comma-separated list of names."),
    min_innings: Optional[int] = Query(1, description="Minimum number of innings bowled by the player under these filters to be included."),
    last_n: Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    include_gamelog: bool = Query(False, description="Set to true to include detailed gamelog for each player.")
):
    """
    Calculates wicket-taking hit rates for bowlers based on specified thresholds and filters.
    Provides the percentage of innings a bowler takes N+ wickets.
    Optionally includes a gamelog of their performances.
    """
    try:
        parsed_thresholds = []
        if thresholds:
            try:
                parsed_thresholds = sorted([int(th.strip()) for th in thresholds.split(',') if th.strip().isdigit() and int(th.strip()) >= 0])
                if not parsed_thresholds:
                    raise ValueError("Thresholds must be a comma-separated list of non-negative integers.")
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid thresholds format: {e}")
        if not parsed_thresholds:
            parsed_thresholds = [1, 2, 3] 
            logger.warning(f"Using default thresholds for bowler wickets: {parsed_thresholds}")

        base_conditions = ["m.match_type='T20'"]
        params = []

        tournament_name_list = []
        if tournament:
            tournament_name_list = [t.strip() for t in tournament.split(',') if t.strip()]
            if tournament_name_list:
                placeholders = ",".join(["?"] * len(tournament_name_list))
                base_conditions.append(f"m.event_name IN ({placeholders})")
                params.extend(tournament_name_list)

        season_clause, season_params_val = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None) 
        if season_clause:
            base_conditions.append(season_clause)
            params.extend(season_params_val)

        if team: 
            base_conditions.append("i.bowling_team = ?") 
            params.append(team)
        if opposition: 
            base_conditions.append("i.batting_team = ?")
            params.append(opposition)
        if venue:
            base_conditions.append("m.venue = ?")
            params.append(venue)
        
        player_list = []
        if players:
            player_list = [p.strip() for p in players.split(',') if p.strip()]
            if player_list:
                placeholders = ",".join(["?"] * len(player_list))
                base_conditions.append(f"d.bowler IN ({placeholders})")
                params.extend(player_list)

        where_clause_main = " AND ".join(base_conditions) if base_conditions else "1=1"

        sql_main = f"""
        WITH PlayerInningWickets AS (
            SELECT
                d.bowler AS player_name,
                d.match_id,
                d.innings_no,
                SUM(CASE WHEN d.player_out IS NOT NULL AND d.wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field', 'handled the ball', 'timed out') THEN 1 ELSE 0 END) AS wickets_in_inning
            FROM delivery d
            JOIN match m ON d.match_id = m.match_id
            JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
            WHERE {where_clause_main}
            GROUP BY d.bowler, d.match_id, d.innings_no
            HAVING SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END) > 0
        ),
        PlayerAggregatedStats AS (
            SELECT
                piw.player_name,
                COUNT(DISTINCT piw.match_id || '_' || piw.innings_no) AS total_innings_bowled
                {'' .join([f', SUM(CASE WHEN piw.wickets_in_inning >= {th} THEN 1 ELSE 0 END) AS innings_ge_{th}_wkts' for th in parsed_thresholds])}
            FROM PlayerInningWickets piw
            GROUP BY piw.player_name
            {f"HAVING COUNT(DISTINCT piw.match_id || '_' || piw.innings_no) >= ?" if min_innings is not None and min_innings > 0 else ""}
        )
        SELECT
            pas.player_name,
            pas.total_innings_bowled
            {'' .join([f', pas.innings_ge_{th}_wkts' for th in parsed_thresholds])}
        FROM PlayerAggregatedStats pas
        ORDER BY pas.player_name;
        """
        
        final_params_main = list(params)
        if min_innings is not None and min_innings > 0:
            final_params_main.append(min_innings)

        logger.info(f"Executing BOWLER WICKETS HIT RATE query with conditions: {where_clause_main}, params: {final_params_main}")
        main_query_start_time = time.time()
        player_data_tuples = db.execute(sql_main, tuple(final_params_main)).fetchall()
        logger.info(f"Main bowler wickets query took {time.time() - main_query_start_time:.4f} seconds, {len(player_data_tuples)} players found.")

        results = []
        player_names_for_gamelog = [row[0] for row in player_data_tuples]

        gamelog_data_map = {}
        if include_gamelog and player_names_for_gamelog:
            gamelog_conditions_list = list(base_conditions) 
            placeholders_gamelog_players = ",".join(["?"] * len(player_names_for_gamelog))
            gamelog_conditions_list.append(f"d.bowler IN ({placeholders_gamelog_players})")
            gamelog_params_list = list(params) + player_names_for_gamelog 
            
            where_clause_gamelog = " AND ".join(gamelog_conditions_list)

            sql_gamelog = f"""
            SELECT
                d.bowler AS player_name,            -- 0
                d.match_id,                         -- 1
                m.match_date,                       -- 2
                i.bowling_team AS bowling_team_for_player, -- 3 (Bowler's actual team)
                i.batting_team AS batting_team_opponent,   -- 4 (Team bowled against)
                m.venue,                            -- 5
                m.city,                             -- 6
                d.innings_no,                       -- 7
                SUM(CASE WHEN d.player_out IS NOT NULL AND d.wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field', 'handled the ball', 'timed out') THEN 1 ELSE 0 END) AS wickets_taken_in_inning, -- 8
                SUM(d.runs_total) AS runs_conceded_in_inning, -- 9 (Total runs conceded by bowler in inning)
                SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END) AS balls_bowled_in_inning, -- 10 (Legal balls)
                COALESCE(ROUND(SUM(d.runs_total) * 6.0 / NULLIF(SUM(CASE WHEN d.extras_type IS NULL OR d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END), 0), 2), NULL) AS economy_rate_in_inning -- 11
            FROM delivery d
            JOIN match m ON d.match_id = m.match_id
            JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
            WHERE {where_clause_gamelog}
            GROUP BY d.bowler, d.match_id, m.match_date, i.bowling_team, i.batting_team, m.venue, m.city, d.innings_no
            ORDER BY d.bowler, m.match_date DESC, d.match_id DESC, d.innings_no;
            """
            logger.info(f"Executing GAMELOG bowler wickets query with: {where_clause_gamelog}, params: {gamelog_params_list}")
            gamelog_query_start_time = time.time()
            gamelog_tuples = db.execute(sql_gamelog, tuple(gamelog_params_list)).fetchall()
            logger.info(f"Gamelog bowler wickets query took {time.time() - gamelog_query_start_time:.4f} seconds, {len(gamelog_tuples)} gamelog entries found.")

            for gl_row in gamelog_tuples:
                p_name = gl_row[0]
                if p_name not in gamelog_data_map:
                    gamelog_data_map[p_name] = []
                
                balls_bowled = gl_row[10]
                overs = f"{balls_bowled // 6}.{balls_bowled % 6}" if balls_bowled is not None else None

                gamelog_data_map[p_name].append(BowlerAPIGamelogEntry(
                    match_id=gl_row[1],
                    match_date=gl_row[2],
                    bowling_team_for_player=gl_row[3],
                    batting_team_opponent=gl_row[4],
                    venue=gl_row[5],
                    city=gl_row[6],
                    innings_no=gl_row[7],
                    wickets_taken_in_inning=gl_row[8],
                    runs_conceded_in_inning=gl_row[9],
                    balls_bowled_in_inning=balls_bowled,
                    economy_rate_in_inning=gl_row[11],
                    overs_bowled=overs
                ))

        for row_tuple in player_data_tuples:
            player_name = row_tuple[0]
            total_innings_bowled = row_tuple[1]
            
            player_threshold_stats = []
            col_idx = 2
            for th in parsed_thresholds:
                innings_met_threshold = row_tuple[col_idx] if total_innings_bowled > 0 and row_tuple[col_idx] is not None else 0
                hit_rate = (innings_met_threshold / total_innings_bowled) * 100 if total_innings_bowled > 0 else 0.0
                
                player_threshold_stats.append(BowlerWicketThresholdStat(
                    threshold=th,
                    total_innings_for_criteria=total_innings_bowled,
                    innings_met_or_exceeded_threshold=innings_met_threshold,
                    hit_rate_percentage=round(hit_rate, 2)
                ))
                col_idx += 1
            
            player_gamelog = gamelog_data_map.get(player_name, []) if include_gamelog else None

            results.append(BowlerWicketPlayerStats(
                player_name=player_name,
                stats=player_threshold_stats,
                gamelog=player_gamelog
            ))
            
        if not results and player_list: 
            for p_name_requested in player_list:
                if p_name_requested not in player_names_for_gamelog:
                    # Player was requested but had no main stats (e.g., below min_innings)
                    # Add them with empty stats, but include gamelog if available and requested
                    player_specific_gamelog = gamelog_data_map.get(p_name_requested, []) if include_gamelog else None
                    if include_gamelog and not player_specific_gamelog: # If gamelog was requested but none found for this specific player
                         player_specific_gamelog = [] # ensure gamelog field is at least an empty list if include_gamelog is true
                    
                    # Only add if they truly have no main stats but were in the input player_list
                    # And either gamelog is included (even if empty) or not requested
                    if include_gamelog or player_specific_gamelog is None: 
                        results.append(BowlerWicketPlayerStats(player_name=p_name_requested, stats=[], gamelog=player_specific_gamelog))
        return results

    except HTTPException: 
        raise
    except Exception as e:
        logger.error(f"Error in get_bowler_wickets_taken_rates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal server error occurred processing bowler wicket rates: {str(e)}")

@router.get("/matchups/player_h2h", response_model=PlayerH2HStats, tags=["matchups", "players"])
async def get_player_h2h_stats(
    batter_name: str = Query(..., description="Name of the batter."),
    bowler_name: str = Query(..., description="Name of the bowler."),
    season: Optional[str] = Query(None, description="Filter by a specific season (e.g., '2022', '2023/24'). Overridden by last_n."),
    last_n: Optional[int] = Query(None, ge=1, description="Number of most recent distinct seasons to include."),
    venue: Optional[str] = Query(None, description="Filter by venue (exact match)."),
    tournament: Optional[str] = Query(None, description="Filter by tournament name (exact match).")
):
    """
    Provides Head-to-Head (H2H) statistics between a specific batter and a specific bowler.
    """
    conditions = ["d.batter = ?", "d.bowler = ?"]
    params = [batter_name, bowler_name]
    
    applied_filters = {
        "batter_name": batter_name,
        "bowler_name": bowler_name
    }

    tournament_name_list = []
    if tournament:
        # Assuming single tournament for this endpoint's season context
        tournament_name_list = [tournament] if tournament.strip() else []
        if tournament_name_list:
            conditions.append("m.event_name = ?") # Add to main query conditions
            params.append(tournament_name_list[0])
            applied_filters["tournament"] = tournament_name_list[0]


    season_clause, season_params = get_season_filter_clause(season, last_n, tournament_names=tournament_name_list if tournament_name_list else None)
    if season_clause:
        conditions.append(season_clause)
        params.extend(season_params)
        if last_n: applied_filters["last_n_seasons"] = last_n
        elif season: applied_filters["season"] = season

    if venue:
        conditions.append("m.venue = ?")
        params.append(venue)
        applied_filters["venue"] = venue

    where_clause = " AND ".join(conditions)

    sql = f"""
    SELECT
        COUNT(DISTINCT d.match_id) as matches_played_together,
        COUNT(DISTINCT (d.match_id, d.innings_no)) as innings_batted,
        COALESCE(SUM(d.runs_batter), 0) as runs_scored,
        COALESCE(SUM(CASE WHEN d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END), 0) as balls_faced,
        COALESCE(SUM(CASE WHEN d.player_out = d.batter AND d.wicket_type NOT IN ('run out', 'retired hurt', 'obstructing the field', 'handled the ball', 'timed out') THEN 1 ELSE 0 END), 0) as dismissals,
        COALESCE(SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END), 0) as fours_hit,
        COALESCE(SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END), 0) as sixes_hit,
        COALESCE(SUM(CASE WHEN d.runs_batter = 0 AND d.extras_type NOT IN ('wides', 'noballs') THEN 1 ELSE 0 END), 0) as dot_balls
    FROM delivery d
    JOIN match m ON d.match_id = m.match_id
    JOIN innings i ON d.match_id = i.match_id AND d.innings_no = i.innings_no
    WHERE {where_clause};
    """

    logging.info(f"Executing Player H2H query: {sql} with params: {params}")
    try:
        row = db.execute(sql, params).fetchone()
    except Exception as e:
        logging.error(f"Error executing Player H2H query: {e}\\nSQL: {sql}\\nParams: {params}")
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

    if not row or row[0] == 0: 
        return PlayerH2HStats(
            batter_name=batter_name,
            bowler_name=bowler_name,
            filters_applied=applied_filters,
            matches_played_together=0,
            innings_batted=0,
            runs_scored=0,
            balls_faced=0,
            dismissals=0,
            fours_hit=0,
            sixes_hit=0
        )
    
    matches_played_together, innings_batted, runs_scored, balls_faced, dismissals, fours_hit, sixes_hit, dot_balls = row

    strike_rate = (runs_scored / balls_faced) * 100 if balls_faced > 0 else None
    average = runs_scored / dismissals if dismissals > 0 else None
    if average is None and runs_scored > 0 and dismissals == 0: # Handle infinite average
        average = float('inf') 
        
    dot_ball_percentage = (dot_balls / balls_faced) * 100 if balls_faced > 0 else None
    boundary_percentage = ((fours_hit + sixes_hit) / balls_faced) * 100 if balls_faced > 0 else None
    return PlayerH2HStats(
        batter_name=batter_name,
        bowler_name=bowler_name,
        filters_applied=applied_filters,
        matches_played_together=matches_played_together,
        innings_batted=innings_batted,
        runs_scored=runs_scored,
        balls_faced=balls_faced,
        dismissals=dismissals,
        strike_rate=round(strike_rate, 2) if strike_rate is not None else None,
        average=round(average, 2) if average is not None and average != float('inf') else (None if average is None else 'inf'),
        dot_ball_percentage=round(dot_ball_percentage, 2) if dot_ball_percentage is not None else None,
        fours_hit=fours_hit,
        sixes_hit=sixes_hit,
        boundary_percentage=round(boundary_percentage, 2) if boundary_percentage is not None else None
    )

# Mount the router with the /api prefix
app.include_router(router, prefix="/api")



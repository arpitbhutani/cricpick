#!/usr/bin/env python3
import json
from datetime import datetime
from pathlib import Path
import duckdb
from tqdm import tqdm

print(f"Using DuckDB version: {duckdb.__version__}") # Verify version

# ─── CONFIG ────────────────────────────────────────────────────────────────
DB_PATH  = Path("/Users/arpitbhutani/Desktop/cristat/cristat.duckdb")
JSON_DIR = Path("/Users/arpitbhutani/Desktop/cristat/data")

# ─── INLINE DDL ─────────────────────────────────────────────────────────────
DDL = """
DROP TABLE IF EXISTS delivery;
DROP TABLE IF EXISTS innings;
DROP TABLE IF EXISTS match;

CREATE TABLE match (
  match_id            TEXT PRIMARY KEY,
  match_date          DATE,
  season              TEXT,
  team_type           TEXT,
  teams               TEXT,
  event_name          TEXT,
  event_match_number  INTEGER,
  gender              TEXT,
  match_type          TEXT,
  venue               TEXT,
  city                TEXT,
  outcome_winner      TEXT,
  outcome_by_runs     INTEGER,
  outcome_by_wickets  INTEGER,
  overs_allocated     INTEGER,
  balls_per_over      INTEGER,
  player_of_match     TEXT
);

CREATE TABLE innings (
  match_id     TEXT,
  innings_no   INTEGER,
  batting_team TEXT,
  bowling_team TEXT
);

CREATE TABLE delivery (
  match_id               TEXT,
  innings_no             INTEGER,
  over_no                INTEGER,
  ball_in_over           INTEGER,
  ball_number_absolute   INTEGER,
  batter                 TEXT,
  bowler                 TEXT,
  non_striker            TEXT,
  runs_batter            INTEGER,
  runs_extras            INTEGER,
  runs_total             INTEGER,
  extras_type            TEXT,
  is_boundary_4          BOOLEAN,
  is_boundary_6          BOOLEAN,
  wicket_type            TEXT,
  player_out             TEXT,
  fielders_involved      TEXT
);
"""

# ─── FLATTEN FUNCTION (using the one we refined before) ───────────────────
def flatten(fp: Path):
    raw  = json.loads(fp.read_text())
    info = raw.get("info", {})
    mid  = fp.stem
    evt     = info.get("event", {})
    outcome = info.get("outcome", {})
    by      = outcome.get("by", {})
    match_row = (
        mid, datetime.fromisoformat(info.get("dates", ["1970-01-01"])[0]).date(),
        info.get("season"), info.get("team_type"), json.dumps(info.get("teams", [])),
        evt.get("name"), evt.get("match_number"), info.get("gender"), info.get("match_type"),
        info.get("venue"), info.get("city"), outcome.get("winner"), by.get("runs"),
        by.get("wickets"), info.get("overs"), info.get("balls_per_over"),
        json.dumps(info.get("player_of_match", []))
    )
    deliveries = []
    innings_rows = []
    ball_counter = 0
    match_teams = info.get("teams", [])
    for inn_no, inn_data in enumerate(raw.get("innings", []), start=1):
        batting_team = inn_data.get("team")
        bowling_team = None
        if len(match_teams) == 2 and batting_team:
            if match_teams[0] == batting_team: bowling_team = match_teams[1]
            elif match_teams[1] == batting_team: bowling_team = match_teams[0]
        innings_rows.append((mid, inn_no, batting_team, bowling_team))
        over_blocks = inn_data.get("overs") or [{"over": None, "deliveries": inn_data.get("deliveries", [])}]
        for ov in over_blocks:
            raw_ov = ov.get("over")
            for idx, ball in enumerate(ov.get("deliveries", []), start=1):
                ball_counter += 1; runs = ball.get("runs", {}); extras = ball.get("extras", {}); w = ball.get("wickets") or []
                kind = w[0].get("kind") if w else None; outp = w[0].get("player_out") if w else None
                fldn = ",".join(f.get("name") if isinstance(f, dict) and "name" in f else str(f) for f in (w[0].get("fielders", []) if w else [])) or None
                if raw_ov is None:
                    bpo = info.get("balls_per_over", 6); over_no = (ball_counter - 1) // bpo
                    ball_in_over = (ball_counter - 1) % bpo + 1
                else: over_no = raw_ov; ball_in_over = idx
                deliveries.append((
                    mid, inn_no, over_no, ball_in_over, ball_counter, ball.get("batter"), ball.get("bowler"),
                    ball.get("non_striker"), runs.get("batter"), runs.get("extras"), runs.get("total"),
                    next(iter(extras), None), runs.get("batter") == 4, runs.get("batter") == 6, kind, outp, fldn ))
    return match_row, innings_rows, deliveries

# ─── MAIN SCRIPT WITH BATCHED EXECUTEMANY ───────────────────────────────────
def main():
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL)
    print("Tables created.")

    fps = sorted(JSON_DIR.glob("**/*.json"))
    print(f"Found {len(fps)} JSON files to process.")

    buffer_size = 500 # Adjust as needed for memory/performance balance
    match_rows_buffer = []
    innings_rows_buffer = []
    delivery_rows_buffer = []

    try:
        con.begin() # Start a single transaction

    for fp in tqdm(fps, desc="Processing JSON files"):
        try:
                mrow, irows, drows = flatten(fp)
                
                match_rows_buffer.append(mrow)
                innings_rows_buffer.extend(irows)
                delivery_rows_buffer.extend(drows)

                # Flush buffers when they reach a certain size
                if len(match_rows_buffer) >= buffer_size:
                    con.executemany("INSERT OR REPLACE INTO match VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", match_rows_buffer)
                    match_rows_buffer.clear()
                
                if len(innings_rows_buffer) >= buffer_size * 5: # Example: innings buffer can be larger
                    con.executemany("INSERT INTO innings VALUES (?, ?, ?, ?)", innings_rows_buffer)
                    innings_rows_buffer.clear()

                if len(delivery_rows_buffer) >= buffer_size * 20: # Example: delivery buffer largest
                    con.executemany("INSERT INTO delivery VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", delivery_rows_buffer)
                    delivery_rows_buffer.clear()

        except Exception as e:
                print(f"Error processing file {fp.name}: {e}")
                # Optionally, decide if you want to skip this file or halt all ingestion

        # Insert any remaining buffered rows after the loop
        if match_rows_buffer:
            con.executemany("INSERT OR REPLACE INTO match VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", match_rows_buffer)
        if innings_rows_buffer:
            con.executemany("INSERT INTO innings VALUES (?, ?, ?, ?)", innings_rows_buffer)
        if delivery_rows_buffer:
            con.executemany("INSERT INTO delivery VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", delivery_rows_buffer)
        
        con.commit() # Commit the transaction if all files processed successfully
        print("✅ Ingestion complete with batched executemany.")

    except Exception as e:
        print(f"Critical error during ingestion: {e}")
        con.rollback() # Rollback transaction on critical error
        print("Transaction rolled back.")
    finally:
        con.close() 

if __name__ == "__main__":
    main()

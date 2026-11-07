# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "-r requirements.txt",
# ]
# ///
# %pip install -r requirements.txt

# COMMAND ----------

import os
import json
import pandas as pd
import openpyxl
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

# COMMAND ----------

catalog = "workspace"
schema = "korfball_statistics"

# COMMAND ----------

def append_to_table(pdf, table_name):
    if table_name == "lineup":
        schema = StructType(
            [
                StructField("match_id", StringType(), False),
                StructField("club_code", StringType(), True),
                StructField("team_name", StringType(), True),
                StructField("PlayerName", StringType(), True),
                StructField("PlayerCode", StringType(), True),
                StructField("PlayerNumber", IntegerType(), True),
                StructField("Gender", StringType(), True),
                StructField("StartZone", StringType(), True),
                StructField("CurrentZone", StringType(), True),
                StructField("IsCaptain", BooleanType(), True),
            ]
        )
    elif table_name == "events":
        schema = StructType(
            [
                StructField("match_id", StringType(), False),
                StructField("Row", IntegerType(), False),
                StructField("Event", StringType(), True),
                StructField("Action", StringType(), True),
                StructField("Description", StringType(), True),
                StructField("PeriodTime", StringType(), True),
                StructField("TeamName", StringType(), True),
                StructField("PlayerName", StringType(), True),
                StructField("Result", StringType(), True),
                StructField("HomeScore", IntegerType(), True),
                StructField("AwayScore", IntegerType(), True),
            ]
        )
    elif table_name == "matches":
        schema = StructType(
            [
                StructField("match_id", StringType(), False),
                StructField("date", StringType(), True),
                StructField("home_team", StringType(), True),
                StructField("away_team", StringType(), True),
            ]
        )
    else:
        raise ValueError(f"Table {table_name} not supported")

    sdf = spark.createDataFrame(pdf, schema)
    sdf.write.mode("append").saveAsTable(f"{catalog}.{schema}.{table_name}")

# COMMAND ----------

def document_tables():
    tables = {
        "lineup": "The line up per match",
        "events": "The events per match",
        "matches": "The matches",
    }
    for table, description in tables.items():
        spark.sql(f"COMMENT ON TABLE {catalog}.{schema}.{table} IS '{description}'")

# COMMAND ----------

# DBTITLE 1,document_columns (syntax fix)
def document_columns():
    tables = {
        "lineup": {
            "match_id": "The link to the match",
            "club_code": "HOME for the home team or AWAY for the away team",
            "team_name": "The team name",
            "PlayerName": "The player name",
            "PlayerCode": "The player code",
            "PlayerNumber": "The player number",
            "Gender": "The gender of the player",
            "StartZone": "FirstAttack when players starts in attack or FirstDefense when players starts in defense or Substitutes when player started as substitute",
            "CurrentZone": "FirstAttack when players starts in attack or FirstDefense when players starts in defense or Substitutes when player started as substitute",
            "IsCaptain": "True for the captain"
        },
        "events": {
            "match_id": "The link to the match",
            "Row": "The order in which the event happened",
            "Event": "The event",
            "Action": "The action",
            "Description": "The description",
            "PeriodTime": "The time in the period, the period time is reset in thew second half",
            "TeamName": "The team name that did the action",
            "PlayerName": "The player name that did the action",
            "Result": "The result of the scoring attempt (Missed or Goal)",
            "HomeScore": "The home score after the event",
            "AwayScore": "The away score after the event"
        },
        "matches": {
            "match_id": "The link to the match",
            "date": "The date of the match",
            "home_team": "The home team",
            "away_team": "The away team"
        },
    }
    for table, columns in tables.items():
        for column, description in columns.items():
            spark.sql(f"ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {column} COMMENT '{description}'")

# COMMAND ----------

def drop_tables():
    tables = ["lineup", "events", "matches"]
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}")

# COMMAND ----------

def analyze_file(file_path, output_folder):
    m = ["", "", "", ""]
    with open(file_path, 'r') as file:
        print(f"analyzing {file_path}")
        json_data = json.load(file)
        match_id = json_data["MatchId"]
        if "LineUp" in json_data:
            df = analyze_lineup(json_data["LineUp"], match_id)
            append_to_table(df,"lineup")

        if "Events" in json_data:
            df = analyze_events(json_data["Events"], match_id)
            append_to_table(df,"events")

        if "HomeTeam" in json_data and "AwayTeam" in json_data:
            df_home = analyze_team(json_data["HomeTeam"], match_id)
            df_away = analyze_team(json_data["AwayTeam"], match_id)
            df = pd.concat([df_home, df_away], ignore_index=True)
            append_to_table(df,"lineup")

        m = analyze_match(json_data, match_id)
    return m

# COMMAND ----------

def analyze_match(match, match_id):
    results = [match_id]
    if "Date" in match:
        results.append(match["Date"])
    elif "StartTime" in match and len(match["StartTime"]) >= 10:
        results.append(match["StartTime"][:10])
    else:
        results.append("unknown")

    if "LineUp" in match:
        results.append(match["LineUp"]["HomeTeam"]["TeamName"])
        results.append(match["LineUp"]["AwayTeam"]["TeamName"])
    elif "HomeTeam" in match and "AwayTeam" in match:
        results.append(match["HomeTeam"]["TeamName"])
        results.append(match["AwayTeam"]["TeamName"])
    else:
        results.append("unknown")
        results.append("unknown")
    return results

# COMMAND ----------

def analyze_events(events, match_id):
    columns = ["Event", "Action", "Description", "PeriodTime", "TeamName", "PlayerName", "Result", "HomeScore",
               "AwayScore"]
    results = []

    e = 0
    for event in events:
        e += 1
        if "Attack" in event and "Actions" in event["Attack"]:
            a = 0
            for action in event["Attack"]["Actions"]:
                a += 1
                results.append(analyze_action(e, a, columns, action))

        results.append(analyze_event(e, columns, event))

    df = pd.DataFrame(results, columns=columns)
    df["HomeScore"] = df["HomeScore"].astype('Int64')
    df["AwayScore"] = df["AwayScore"].astype('Int64')
    df.insert(0, "Row", range(1, len(df) + 1))
    df.insert(0, "match_id", match_id)
    return df

# COMMAND ----------

def analyze_event(event_nr, columns, event):
    results = []
    for column in columns:
        if column == "Event":
            results.append(f"E{event_nr:03d}")
        elif column == "Description":
            results.append(event["Event"])
        elif column in event:
            results.append(event[column])
        else:
            results.append(None)
    return results

# COMMAND ----------

def analyze_action(event_nr, action_nr, columns, action):
    results = []
    for column in columns:
        if column == "Event":
            results.append(f"E{event_nr:03d}")
        elif column == "Action":
            results.append(f"A{action_nr:03d}")
        elif column == "Description":
            results.append(action["Action"])
        elif column in action:
            results.append(action[column])
        else:
            results.append(None)
    return results

# COMMAND ----------

def analyze_lineup(lineup, match_id):
    df = None
    if "HomeTeam" in lineup:
        df = analyze_team(lineup["HomeTeam"], match_id)
    if "AwayTeam" in lineup:
        df_away = analyze_team(lineup["AwayTeam"], match_id)
        if df is None:
            df = df_away
        else:
            df = pd.concat([df, df_away], ignore_index=True)
    return df

# COMMAND ----------

def analyze_team(team, match_id):
    columns = ["PlayerName", "PlayerCode", "PlayerNumber", "Gender", "StartZone", "CurrentZone", "IsCaptain"]
    team_name = team["TeamName"]
    club_code = team["ClubCode"]
    results = []
    for player in team["Players"]:
        results.append(analyze_player(player, columns))

    df = pd.DataFrame(results, columns=columns)
    df.insert(0, "team_name", team_name)
    df.insert(0, "club_code", club_code)
    df.insert(0, "match_id", match_id)
    return df

# COMMAND ----------

def analyze_player(player, columns):
    results = []
    for column in columns:
        if column in player:
            results.append(player[column])
        else:
            results.append(None)
    return results

# COMMAND ----------

# DBTITLE 1,Add error handling for Excel write error
def refresh_data():
    drop_tables()
    data_folder = "/Volumes/workspace/korfball_statistics/data/raw"
    output_folder = "/Volumes/workspace/korfball_statistics/data/processed"
    matches = []
    for f in os.listdir(data_folder):
        if f.endswith(".json"):
            file_path = f"{data_folder}/{f}"
            match = analyze_file(file_path, output_folder)
            matches.append(match)

    match_file_name = f"{output_folder}/matches.csv"
    df = pd.DataFrame(matches, columns=["match_id", "date", "home_team", "away_team"])
    append_to_table(df, "matches")

# COMMAND ----------

# refresh_data()
# document_tables()
# document_columns()
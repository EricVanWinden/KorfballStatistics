import os
import json
import pandas as pd
import openpyxl

print(f"json: {pd.__version__}")
print(f"pandas: {pd.__version__}")
print(f"openpyxl: {openpyxl.__version__}")


def analyze_file(file_path, output_folder):
    m = ["", "", "", ""]
    with open(file_path, 'r') as file:
        print(f"analyzing {file_path}")
        json_data = json.load(file)
        match_id = json_data["MatchId"]
        if "LineUp" in json_data:
            df = analyze_lineup(json_data["LineUp"], match_id)
            lineup_file_name = f"{output_folder}/{match_id}_lineup.xlsx"
            print(f"Saving lineup in {lineup_file_name}")
            df.to_excel(lineup_file_name, index=False)

        if "Events" in json_data:
            df = analyze_events(json_data["Events"], match_id)
            events_file_name = f"{output_folder}/{match_id}_events.xlsx"
            print(f"Saving events in {events_file_name}")
            df.to_excel(events_file_name, index=False)

        if "HomeTeam" in json_data and "AwayTeam" in json_data:
            df_home = analyze_team(json_data["HomeTeam"], match_id)
            df_away = analyze_team(json_data["AwayTeam"], match_id)
            df = pd.concat([df_home, df_away], ignore_index=True)
            lineup_file_name = f"{output_folder}/{match_id}_lineup.xlsx"
            print(f"Saving lineup in {lineup_file_name}")
            df.to_excel(lineup_file_name, index=False)

        m = analyze_match(json_data, match_id)
    return m


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
    df.insert(0, "Row", range(1, len(df) + 1))
    df.insert(0, "match_id", match_id)
    return df


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
            results.append("")
    return results


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
            results.append("")
    return results


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


def analyze_player(player, columns):
    results = []
    for column in columns:
        if column in player:
            results.append(player[column])
        else:
            results.append("")
    return results


if __name__ == "__main__":
    data_folder = "D:/Eric/VSTS/KorfballStatistics/data"
    output_folder = "D:/Eric/VSTS/KorfballStatistics/output"
    matches = []
    for f in os.listdir(data_folder):
        if f.endswith(".json"):
            file_path = f"{data_folder}/{f}"
            match = analyze_file(file_path, output_folder)
            matches.append(match)

    match_file_name = f"{output_folder}/matches.xlsx"
    print(f"Saving matches in {match_file_name}")
    df = pd.DataFrame(matches, columns=["match_id", "date", "home_team", "away_team"])
    df.to_excel(match_file_name, index=False)

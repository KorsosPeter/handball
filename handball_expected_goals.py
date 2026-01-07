import sys
import time
import csv
import json
import requests
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

BASE_URL = "https://api.sofascore.com/api/v1"
SPORT = "handball"


@dataclass
class TeamStats:
    team_id: int
    name: str
    season_id: int
    goals_scored: int
    matches_played: int

    @property
    def goals_per_match(self) -> Optional[float]:
        if self.matches_played <= 0:
            return None
        return self.goals_scored / self.matches_played


@dataclass
class MatchInfo:
    event_id: int
    start_timestamp: int
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str
    tournament_name: str
    season_id: int


@dataclass
class MatchWithExpectedGoals:
    match: MatchInfo
    home_stats: Optional[TeamStats]
    away_stats: Optional[TeamStats]
    exp_goals_home: Optional[float]
    exp_goals_away: Optional[float]
    joint_expected_goals: Optional[float]


def get_json(url: str, params: Dict = None, retries: int = 3, sleep: float = 0.5):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
        except requests.RequestException:
            pass
        time.sleep(sleep)
    return None


def get_scheduled_events(date_str: str) -> List[MatchInfo]:
    url = f"{BASE_URL}/sport/{SPORT}/scheduled-events/{date_str}"
    data = get_json(url)
    if not data or "events" not in data:
        return []

    matches: List[MatchInfo] = []
    for ev in data["events"]:
        try:
            # Sofascore changed the structure; do NOT filter by sport
            season_id = ev.get("season", {}).get("id")
            if season_id is None:
                continue

            matches.append(
                MatchInfo(
                    event_id=ev["id"],
                    start_timestamp=ev.get("startTimestamp", 0),
                    home_team_id=ev["homeTeam"]["id"],
                    home_team_name=ev["homeTeam"]["name"],
                    away_team_id=ev["awayTeam"]["id"],
                    away_team_name=ev["awayTeam"]["name"],
                    tournament_name=ev.get("tournament", {}).get("name", "Unknown"),
                    season_id=season_id,
                )
            )
        except KeyError:
            continue

    return matches

def get_team_stats(team_id: int, season_id: int) -> Optional[TeamStats]:
    url = f"{BASE_URL}/team/{team_id}/statistics/seasons/{season_id}"
    data = get_json(url)
    if not data:
        return None

    try:
        stats = data.get("statistics", {})
        goals_scored = (
            stats.get("goalsScored")
            or stats.get("goalsFor")
            or stats.get("scored")
            or 0
        )
        matches_played = (
            stats.get("matchesPlayed")
            or stats.get("played")
            or stats.get("games")
            or 0
        )

        return TeamStats(
            team_id=team_id,
            name=data.get("team", {}).get("name", f"Team {team_id}"),
            season_id=season_id,
            goals_scored=goals_scored,
            matches_played=matches_played,
        )
    except Exception:
        return None


def compute_expected_goals_for_matches(matches: List[MatchInfo]) -> List[MatchWithExpectedGoals]:
    stats_cache: Dict[Tuple[int, int], Optional[TeamStats]] = {}
    results: List[MatchWithExpectedGoals] = []

    for m in matches:
        key_home = (m.home_team_id, m.season_id)
        key_away = (m.away_team_id, m.season_id)

        if key_home not in stats_cache:
            stats_cache[key_home] = get_team_stats(m.home_team_id, m.season_id)
            time.sleep(0.2)

        if key_away not in stats_cache:
            stats_cache[key_away] = get_team_stats(m.away_team_id, m.season_id)
            time.sleep(0.2)

        home_stats = stats_cache[key_home]
        away_stats = stats_cache[key_away]

        exp_home = home_stats.goals_per_match if home_stats else None
        exp_away = away_stats.goals_per_match if away_stats else None

        joint = exp_home + exp_away if exp_home is not None and exp_away is not None else None

        results.append(
            MatchWithExpectedGoals(
                match=m,
                home_stats=home_stats,
                away_stats=away_stats,
                exp_goals_home=exp_home,
                exp_goals_away=exp_away,
                joint_expected_goals=joint,
            )
        )

    return results


def export_to_csv(matches: List[MatchWithExpectedGoals], date_str: str):
    filename = f"handball_expected_goals_{date_str}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Home Team",
            "Away Team",
            "Tournament",
            "Expected Goals Home",
            "Expected Goals Away",
            "Joint Expected Goals",
            "Home Goals Scored",
            "Home Matches Played",
            "Away Goals Scored",
            "Away Matches Played",
        ])

        for m in matches:
            if m.joint_expected_goals is None:
                continue

            writer.writerow([
                m.match.home_team_name,
                m.match.away_team_name,
                m.match.tournament_name,
                f"{m.exp_goals_home:.2f}" if m.exp_goals_home else "",
                f"{m.exp_goals_away:.2f}" if m.exp_goals_away else "",
                f"{m.joint_expected_goals:.2f}",
                m.home_stats.goals_scored if m.home_stats else "",
                m.home_stats.matches_played if m.home_stats else "",
                m.away_stats.goals_scored if m.away_stats else "",
                m.away_stats.matches_played if m.away_stats else "",
            ])


def export_to_json(matches: List[MatchWithExpectedGoals], date_str: str):
    filename = "handball_expected_goals.json"

    output = []
    for m in matches:
        if m.joint_expected_goals is None:
            continue

        output.append({
            "home_team": m.match.home_team_name,
            "away_team": m.match.away_team_name,
            "tournament": m.match.tournament_name,
            "exp_goals_home": m.exp_goals_home,
            "exp_goals_away": m.exp_goals_away,
            "joint_expected_goals": m.joint_expected_goals,
            "home_goals_scored": m.home_stats.goals_scored if m.home_stats else None,
            "home_matches_played": m.home_stats.matches_played if m.home_stats else None,
            "away_goals_scored": m.away_stats.goals_scored if m.away_stats else None,
            "away_matches_played": m.away_stats.matches_played if m.away_stats else None,
        })

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)


def main():
    if len(sys.argv) < 2:
        print("Usage: python handball_expected_goals.py YYYY-MM-DD")
        sys.exit(1)

    date_str = sys.argv[1]

    matches = get_scheduled_events(date_str)
    matches_with_xg = compute_expected_goals_for_matches(matches)

    export_to_csv(matches_with_xg, date_str)
    export_to_json(matches_with_xg, date_str)


if __name__ == "__main__":
    main()

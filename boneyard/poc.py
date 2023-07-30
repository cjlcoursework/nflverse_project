from random import shuffle, choice
from enum import Enum
from typing import List
from itertools import permutations, combinations
from scipy.stats import truncnorm
import numpy as np


class Milestone(Enum):
    NONE = 1
    TD = 2
    FIRST_DOWN = 3
    TURNOVER = 4
    FIELD_GOAL = 5
    PUNT = 6


class PlayCall(Enum):
    PASS = 1
    RUSH = 2
    PUNT = 3
    FIELD_GOAL = 4


class Position(Enum):
    PASS = 1
    RUSH = 2
    PASS_DEFENSE = 3
    RUSH_DEFENSE = 4


weights = {
    Position.PASS: 3,
    Position.PASS_DEFENSE: 3,
    Position.RUSH: 3,
    Position.RUSH_DEFENSE: 3,
}


class GameStats:
    def __init__(self, game_id):
        self.game_id = game_id
        self.drives = 0
        self.plays = 0
        self.first_downs = 0
        self.yards = 0
        self.touchdowns = 0
        self.pass_plays = 0
        self.rushing_plays = 0
        self.turnovers = 0
        self.field_goals = 0
        self.score = 0
        self.punts = 0


class Player:
    def __init__(self, position: Position, power_score: float):
        self.position = position
        self.player_power = power_score


class Team:
    def __init__(self, name: str, players: List[Player], bench_strength: float):
        self.name = name
        self.players = players
        self.bench_strength = bench_strength
        self.positions = {}
        self.positions = {player.position: player.player_power * weights[player.position] for player in players}
        arr = np.array(list(self.positions.values()))
        self.power_score = np.sum(arr)

    def get_position_powers(self, position: Position):
        return self.positions[position]

    def get_playcall_power(self, play_call: PlayCall):
        if play_call == PlayCall.PASS:
            return self.positions[Position.PASS], self.positions[Position.PASS_DEFENSE]

        if play_call == PlayCall.RUSH:
            return self.positions[Position.RUSH], self.positions[Position.RUSH_DEFENSE]

    def get_position_power(self, position: Position):
        return self.positions[position]


class League:
    def __init__(self):
        self.crews = {}
        self.points_per_match = 40
        self.drive_count = 1

    def add_crew(self, c: Team):
        self.crews[c.name] = c

    def simulate_play(self, down, offense_power: int, defense_power: int, play_call: PlayCall):

        min_value = 0
        max_value = 40
        avg_yard_per_carry = 2
        mean = max(0, avg_yard_per_carry + (offense_power - defense_power) * 4)
        std_dev = 10

        # Calculate the parameters for the truncnorm function
        a = (min_value - mean) / std_dev
        b = (max_value - mean) / std_dev

        # Generate a single random sample from the truncated normal distribution
        sample = int(truncnorm.rvs(a, b, loc=mean, scale=std_dev, size=1))

        return sample

    def play_call_probability(self, offense_power, defense_power):
        difference = offense_power - defense_power
        probability = 1 / (1 + np.exp(-difference))
        return probability

    def field_goal_probability(self, yards):
        if yards < 20:
            return 0.95
        elif yards < 30:
            return 0.9
        elif yards < 40:
            return 0.8
        elif yards < 50:
            return 0.7
        elif yards < 60:
            return 0.25
        else:
            return 0.0

    def gain_yards_probability(self, avg_yards_per_carry, offense_power, defense_power, target_yards) -> float:

        difference = offense_power - defense_power
        success_probability = 1 / (1 + np.exp(-difference))

        success_prob_target_yards = 1 - np.exp(-target_yards / avg_yards_per_carry)

        adjusted_probability = success_prob_target_yards * success_probability

        if isinstance(adjusted_probability, float):
            return adjusted_probability

        return adjusted_probability[0]

    def play_call(self, drive, down, yards_to_goal, yards_to_first, stats, offense_team: Team, defense_team: Team):
        avg_yard_per_carry = 2
        yards_needed = min(yards_to_goal, yards_to_first)
        points = 0
        yards=0
        milestone = Milestone.NONE
        offense_score = stats[offense_team.name].score
        defense_score = stats[defense_team.name].score
        spread = offense_score - defense_score

        number_of_game_drives = 12
        average_points_per_game = 21

        drives_left = number_of_game_drives - drive
        spread = offense_score - defense_score
        defense_possible_points = (defense_score / drive) * drives_left
        offense_possible_points = (offense_score / drive) * drives_left
        win_potential = spread + offense_possible_points - defense_possible_points + 3

        pass_probability = self.gain_yards_probability(
            avg_yards_per_carry=avg_yard_per_carry,
            offense_power=offense_team.get_position_power(Position.PASS),
            defense_power=defense_team.get_position_powers(Position.PASS_DEFENSE),
            target_yards=yards_needed
        )
        rush_probability = self.gain_yards_probability(
            avg_yards_per_carry=avg_yard_per_carry,
            offense_power=offense_team.get_position_power(Position.RUSH),
            defense_power=defense_team.get_position_powers(Position.RUSH_DEFENSE),
            target_yards=yards_needed
        )

        field_goal_probability = 0
        punt_probability=0

        if down == 4:
            field_goal_probability = self.field_goal_probability(yards_to_goal)
            if field_goal_probability < .6:
                if win_potential > 0:
                    punt_probability = 0.9

        if punt_probability > 0:
            milestone = Milestone.PUNT
            return PlayCall.PUNT, offense_team.power_score, defense_team.power_score, yards, points, Milestone.PUNT

        elif field_goal_probability > max(rush_probability, pass_probability):
            points = np.random.choice([0,3], p=[1-field_goal_probability,field_goal_probability])
            points = 3
            milestone = Milestone.FIELD_GOAL
            return PlayCall.FIELD_GOAL, offense_team.power_score, defense_team.power_score, yards, points, Milestone.FIELD_GOAL

        else:

            # if this is a 4th down we have different rules
            d = pass_probability + rush_probability
            probabilities = [pass_probability / d, rush_probability / d]
            play_call = np.random.choice([PlayCall.PASS, PlayCall.RUSH], p=probabilities)

            offense_power, _ = offense_team.get_playcall_power(play_call=play_call)
            _, defense_power = defense_team.get_playcall_power(play_call=play_call)

            min_value = 0
            max_value = 40
            mean = max(0, avg_yard_per_carry + (offense_power - defense_power) * 4)
            std_dev = 10

            # Calculate the parameters for the truncnorm function
            a = (min_value - mean) / std_dev
            b = (max_value - mean) / std_dev

            # Generate a single random sample from the truncated normal distribution
            yards = truncnorm.rvs(a, b, loc=mean, scale=std_dev, size=1)
            if not isinstance(yards, float):
                yards = yards[0]

            if yards >= yards_to_goal:
                points = 7
                milestone = Milestone.TD
            elif yards >= yards_to_first:
                points = 0
                milestone = Milestone.FIRST_DOWN

            return play_call, offense_power, defense_power, yards, points, milestone

    def simulate_drive(self, season, week, game_id, drive, offense_team, defense_team, allstats):

        play_counter = 0
        pbp = {}
        yardline = 25
        yards_to_first = 10
        total_yards = 0
        score = 0
        down = 1
        yards_to_td = 360 - yardline
        turnover = False
        stats = allstats[offense_team.name]
        stats.drives += 1

        while not turnover and score == 0:
            # setup
            play_counter += 1
            play_call, offense_power, defense_power, yards, score, milestone = self.play_call(
                drive=drive,
                down=down,
                yards_to_first=yards_to_first,
                yards_to_goal=yards_to_td,
                stats=allstats,
                offense_team=offense_team,
                defense_team=defense_team)
            # play
            pbp_record = dict(
                season=season,
                week=week,
                game_id=game_id,
                drive=drive,
                down=down,
                offense=offense_team.name,
                offense_power=offense_power,
                offense_score=allstats[offense_team.name].score,
                defense=defense_team.name,
                defense_power=defense_power,
                defense_score=allstats[defense_team.name].score,
                playcall=play_call.name,
                yards=yards,
                yardline=yardline,
                yards_to_first=yards_to_first,
                yards_to_td=yards_to_td,
                yards_gained=total_yards,
                points=score,
                milestone=milestone.name)

            stats.score += score
            stats.yards += yards
            stats.plays += 1
            stats.punts += 1 if milestone == Milestone.PUNT else 0
            stats.first_downs += 1 if milestone == Milestone.FIRST_DOWN else 0
            stats.touchdowns += 1 if milestone == Milestone.TD else 0
            stats.field_goals += 1 if milestone == Milestone.FIELD_GOAL else 0
            stats.pass_plays += 1 if play_call == PlayCall.PASS else 0
            stats.rushing_plays += 1 if play_call == PlayCall.RUSH else 0

            # result
            down += 1
            total_yards += yards
            yardline += yards
            yards_to_td = 360 - yardline
            yards_to_first -= yards

            if yards_to_first <= 0:  # first down
                down = 1
                yards_to_first = 10
            elif down > 4 and milestone == Milestone.NONE:
                pbp_record['milestone'] = Milestone.TURNOVER
                turnover = True  # turnover on downs
                stats.turnovers += 1
            pbp[play_counter] = pbp_record
            print(pbp_record)

        return pbp, allstats

    def get_team_stats(self, team: Team, side: str, score, detail=True):

        game = {
            f"{side}_name": team.name,
            f"{side}_bench": team.bench_strength,
            f"{side}_power": team.power_score,
            f"{side}_score": score,
        }
        if detail:
            game.update(team.get_position_powers(side))
        return game

    def play_match(self, season, week, game_id, home_name, away_name, detail=True):
        lineup = [home_name, away_name]
        shuffle(lineup)
        stats = {home_name: GameStats(game_id), away_name: GameStats(game_id)}
        offense = 0
        game_stats = {}
        drive = 0

        while drive < 12 or stats[home_name].score == stats[away_name].score:
            drive += 1
            offense_name = lineup[offense]
            defense = not offense
            defense_name = lineup[defense]
            offense_team = self.crews[offense_name]
            defense_team = self.crews[defense_name]
            drive_stats, stats = self.simulate_drive(season, week, game_id, drive, offense_team, defense_team, stats)
            game_stats[drive] = drive_stats
            offense = defense

        return game_stats, stats

    def play_game(self, season, week, home_name, away_name, detail=True):

        home: Team = self.crews[home_name]
        away: Team = self.crews[away_name]

        values = np.array([home.power_score, away.power_score])

        percentages = values / np.sum(values)
        points_this_match = np.random.randint(self.points_per_match - 10, self.points_per_match + 10)
        home_score, away_score = [int(p * points_this_match) for p in percentages]

        base = dict(season=season, week=week)

        home_pov = {
            **base,
            **self.get_team_stats(team=home, side="team", score=home_score, detail=detail),
            **self.get_team_stats(team=away, side="opponent", score=away_score, detail=detail)
        }

        away_pov = {
            **base,
            **self.get_team_stats(team=away, side="team", score=away_score, detail=detail),
            **self.get_team_stats(team=home, side="opponent", score=home_score, detail=detail)
        }

        return [home_pov, away_pov]

    def generate_seasons(self, start_season: int, end_season: int):
        seasons_result = {}
        for season in range(start_season, end_season + 1):
            seasons_result[season] = self.generate_season(season)
        return seasons_result

    def generate_season(self, season: int):
        games = {}
        teams = list(self.crews.keys())
        shuffle(teams)
        for week, (home_name, away_name) in enumerate(permutations(teams, 2)):
            game_id = f"""{season}_{week + 1}_{home_name}_{away_name}"""
            game_records, game_scores = self.play_match(season=season,
                                                        week=week + 1,
                                                        game_id=game_id,
                                                        home_name=home_name,
                                                        away_name=away_name, detail=False)

            games[game_id] = {**game_scores, **dict(pbp=game_records)}
            print("")

        return games


if __name__ == "__main__":
    teams = ['LAC', 'KC', 'GB']
    league = League()
    for team in teams:
        league.add_crew(Team(
            name=team,
            bench_strength=np.random.uniform(.01, .05),
            players=[
                Player(Position.PASS_DEFENSE, np.random.uniform(1, 4)),
                Player(Position.PASS, np.random.uniform(1, 4)),
                Player(Position.RUSH, np.random.uniform(1, 4)),
                Player(Position.RUSH_DEFENSE, np.random.uniform(1, 4))
            ]
        ))

    # Get all combinations

    seasons = league.generate_seasons(2020, 2023)
    for season, games in seasons.items():
        print(f"-----\n{season}")
        for game in games:
            print(game)

    print("bye")

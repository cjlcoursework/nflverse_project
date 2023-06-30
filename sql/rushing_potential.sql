--
-- Stats
--

with rushing as (select season,
                        week,
                        carries,
                        rushing_yards,
                        rushing_yards / carries as yards_per_carry,
                        rushing_tds,
                        rushing_first_downs,
                        rushing_2pt_conversions,
                        rushing_fumbles,
                        rushing_epa
                 from player_stats
                 where player_id = '00-0033293')
select season,
       sum(carries) as carries,
       sum(rushing_yards) as rushing_yards,
       min(yards_per_carry) as min_yards_per_carry,
       avg(yards_per_carry) as avg_yards_per_carry,
       max(yards_per_carry) as max_yards_per_carry,
       sum(rushing_tds) as rushing_tds,
       sum(rushing_first_downs) as rushing_first_downs,
       sum(rushing_2pt_conversions) as rushing_2pt_conversions,
       sum(rushing_fumbles) as rushing_fumbles,
       avg(rushing_epa) as rushing_epa

from rushing
group by season;


select season, sum(carries) as carries, sum(rushing_yards), sum(rushing_yards)/sum(carries) as yards_per_carry  from player_stats
where player_id = '00-0033293'
group by season
order by season desc;



--
-- Plays
--
create table offense as
SELECT play_id, unnest(string_to_array(players_on_play, ';')) AS split_value
FROM pbp_participation;

SELECT play_id, unnest(string_to_array(offense_players, ';')) AS split_value
FROM pbp_participation;

SELECT play_id, unnest(string_to_array(defense_players, ';')) AS split_value
FROM pbp_participation;


select nflverse_game_id, play_id from pbp_participation where play_id = 40;






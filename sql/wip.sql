with play_actions as (
    select season, week, game_id,
           drive,
           play_counter,
           play_id,
           posteam,
           posteam_score::integer,
           posteam_score_post::integer,
           defteam,
           defteam_score,
           defteam_score_post,
           (posteam_score - defteam_score) as point_differential,
           down,
           yrdln,
           SUBSTRING(yrdln FROM '[0-9]+')::integer as yard_line,
           SUBSTRING(yrdln FROM '[A-Za-z]+') AS side_of_field,
           ydstogo,
           game_seconds_remaining,
           action,
           yards_gained,
           row_number() over (partition by
               season,
               week,
               play_counter)                 as rn
    from controls.play_actions
    where action in (
                     'extra_point',
                     'field_goal',
                     'pass',
                     'rush')
    )
select
    season,
    week,
    game_id,
    drive,
    play_counter,
    posteam,
    posteam_score,
    posteam_score_post,
    defteam_score,
    defteam_score_post,
    point_differential,
    LEAD(posteam_score) OVER (PARTITION BY posteam ORDER BY season, week, play_counter) AS next_starting_score,
    down,
    ydstogo,
    case when posteam = side_of_field then 100 - yard_line else yard_line end yards_to_goal,
    game_seconds_remaining,
    action,
    yards_gained
--     case when next_starting_score > 0 then
--         next_starting_score - posteam_score
--     else
--         posteam_score_post - posteam_score
--     end as points_gained

from play_actions
where rn = 1
and game_id = '2016_01_BUF_BAL' and drive=7
order by season,
         week,
         game_id,
         play_counter;



WITH play_actions AS (
    select season, week, game_id, play_id,
           drive,
           play_counter,
           posteam,
           posteam_score::integer,
           posteam_score_post::integer,
           defteam,
           defteam_score,
           defteam_score_post,
           (posteam_score - defteam_score) as point_differential,
           down,
           yrdln,
           SUBSTRING(yrdln FROM '[0-9]+')::integer as yard_line,
           SUBSTRING(yrdln FROM '[A-Za-z]+') AS side_of_field,
           ydstogo,
           game_seconds_remaining,
           action,
           yards_gained,
           row_number() over (partition by
               season,
               week,
               play_counter)                 as rn
    from controls.play_actions
    where action in (
                     'extra_point',
                     'field_goal',
                     'pass',
                     'rush')
    -- your original query here
),
next_starting_scores AS (
     SELECT
          *,
         LEAD(posteam_score) OVER (PARTITION BY posteam ORDER BY season, week, play_counter) AS next_starting_score
     FROM play_actions
     WHERE rn = 1
 )
SELECT
    pa.season,
    pa.week,
    pa.game_id,
    pa.drive,
    pa.play_counter,
    pa.posteam,
    pa.posteam_score,
    pa.posteam_score_post,
    pa.defteam_score,
    pa.defteam_score_post,
    pa.point_differential,
    ns.next_starting_score,
    pa.down,
    pa.ydstogo,
    CASE WHEN pa.posteam = pa.side_of_field THEN 100 - pa.yard_line ELSE pa.yard_line END AS yards_to_goal,
    pa.game_seconds_remaining,
    pa.action,
    pa.yards_gained,
    CASE WHEN ns.next_starting_score > 0 THEN ns.next_starting_score - pa.posteam_score ELSE pa.posteam_score_post - pa.posteam_score END AS points_gained
FROM play_actions AS pa
         JOIN next_starting_scores AS ns ON pa.play_id = ns.play_id
WHERE pa.rn = 1
  AND pa.game_id = '2016_01_BUF_BAL' and pa.drive = 7
ORDER BY pa.season, pa.week, pa.game_id, pa.play_counter;

-- 2016_01_BUF_BAL

--
-- select action, count(*)
-- from controls.play_actions
-- group by action;
--
-- with defense_indicators as (select team,
--                                    season,
--                                    week,
--                                    qb_hit,
--                                    tackle,
--                                    ps_interceptions,
--                                    sacks,
--                                    sack_yards,
--                                    row_number() over (partition by team,
--                                        season,
--                                        week) as rn
--                             from nfl_weekly_stats
--                             order by team, season, week)
-- select *
-- from defense_indicators
-- where rn = 1;
--
--
--
-- with offense_indicators as (select team,
--                                    season,
--                                    week,
--                                    carries,
--                                    touchdown,
--                                    passer_rating,
--                                    rushing_first_downs,
--                                    passing_air_yards,
--                                    completion_percentage,
--                                    receiving_air_yards,
--                                    row_number() over (partition by team,
--                                        season,
--                                        week) as rn
--                             from nfl_weekly_stats
--                             order by team, season, week)
-- select *
-- from offense_indicators
-- where rn = 1;
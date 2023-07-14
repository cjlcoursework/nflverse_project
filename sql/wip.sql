CREATE INDEX idx_play_acton_combined ON controls.play_actions (season, week, posteam, defteam);
CREATE INDEX idx_power_scores_combined ON controls.power_scores (season, week, team);

CREATE TABLE controls.pbp_playcall_metrics AS
WITH play_actions AS (
    -- select play by play data from the play_action table
    -- this will be our base table for building a play call dataset
    -- get rid of clock events, penalties and other features that are not pass, rush, field goal
    -- for example, for this dataset a PUNT is just a turnover - it's an important play call
    --     but it's really just a way of turning the ball over - if we don't punt, the action keeps going
    --     so the fact that we punted or did not punt on forth down is already captured in the fact that we are running a play on 4th down
    --     in this dataset we are interested in actions and their resulting rewards
    -- todo - we do need to get 2pt conversion into this set
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
           SUBSTRING(yrdln FROM '[0-9]+')::integer as yard_line,   -- we'll split yrdln column e.g. 'BAL 25' --> 25
           SUBSTRING(yrdln FROM '[A-Za-z]+') AS side_of_field,  -- we'll split yrdln column e.g. 'BAL 25' --> BAL
           ydstogo,
           game_seconds_remaining,
           action,
           yards_gained,
           row_number() over (partition by     -- make sure we are not creating unwanted records
               season,
               week,
               play_counter) as rn
    from controls.play_actions
    where action in (
                     'extra_point',
                     'field_goal',
                     'pass',
                     'rush')
),
next_starting_scores AS (
    -- intermediate step:
    -- get the score from the next down into the current record
    -- assign a unique row_id so we can validate it only occurs once in the final dataset
    -- todo - validate the LEAD function - it works, but perhaps season and week should be in the partition instead of the order
     SELECT
          *,
          ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_id,
          LEAD(posteam_score) OVER (PARTITION BY season, week, posteam, drive ORDER BY  play_counter) AS next_starting_score
     FROM play_actions
     WHERE rn = 1
     ORDER BY season, week, game_id, play_counter
 ),
pbp_actions as (
    -- almost final step :
    -- join everything to this point together
    -- but don't join in offense and defense powers here because we want to be abe to validate during development that this much does not fan out
    -- calc yards_to_goal e.g. if we are on our own side of the field then yards-to-goal is 100 minus the yard line
    SELECT
        ns.row_id,
        pa.season,
        pa.week,
        pa.game_id,
        pa.drive,
        pa.play_counter,
        pa.posteam,
        pa.posteam_score,
        pa.posteam_score_post,
        pa.defteam,
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
        CASE WHEN ns.next_starting_score is not null THEN ns.next_starting_score - pa.posteam_score ELSE pa.posteam_score_post - pa.posteam_score END AS points_gained
    FROM play_actions AS pa
             JOIN next_starting_scores AS ns ON pa.play_id = ns.play_id
    WHERE pa.rn = 1

)
-- finally, we merge in the offense power for the team in possession of the ball and the defense power for the other team
-- this will flip every drive
-- we calculated offense and defense scores as the weighted averages of the features that XGBoost and our shallow neural network told us were important factor in winning games
-- of course, that choice to rollup the features into offense and defense powers is open to further validation
select pa.*, ps.offense_power, ds.defense_power
from pbp_actions pa
         JOIN controls.power_scores ps ON (ps.season = pa.season and ps.week=pa.week and ps.team = pa.posteam)
         JOIN controls.power_scores ds ON (ds.season = pa.season and ds.week=pa.week and ds.team = pa.defteam)
 ORDER BY pa.season, pa.week, pa.game_id, pa.play_counter;


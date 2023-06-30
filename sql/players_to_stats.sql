---
--- Join players with player_stats
---
--- players is just information primarily, so player_stats is key, and they look good
---  we'll just backfill or delete
--- In an automated data stream
---

--
-- GATES
--

-- season should never be null
select count(*) from player_stats where season is null;

-- week should never be null
select count(*) from player_stats where week is null;

--- player_id should not be null
select * from player_stats where player_id is null;

--
-- IMPUTATION
--

-- 69 players with null status
select count(*) from players where status is null;
--     fill missing status in players with a 'NOT-STATED' field - we won't really be predicting on this, so its ok


-- 500 players with no stats: no problem, leave it alone
with pl as (
    select p.gsis_id, ps.player_id , p.status as status, p.season from players p
    full outer join player_stats ps on p.gsis_id = ps.player_id  )
select pl.status, count(*) from pl where gsis_id is not null and player_id is  null
group by pl.status
;

-- player_stats with no players
with pl as (select p.gsis_id, ps.player_id , p.status, ps.season from players p
                full outer join player_stats ps on p.gsis_id = ps.player_id )
select player_id, season, count(*) from pl
where gsis_id is null and player_id is not null
group by season, player_id
;

-- if the season is 1999 or older and the count is less than n, this is ok
-- when and if we join with players these records would drop out









---
--- Check players and check join with player_stats
---
--- players is just information primarily, so player_stats is key, and they look good
---  we'll just backfill or delete
--- In an automated data stream
---

--
-- GATES
--
select count(*) from players where gsis_id is null;
-- should be just a few, maybe 2 or 3

select count(*) from players where status is null;
--  there should be under 70

-- players with no player_stats -- this is fine

-- player_stats with no players  -- should only be 1999 or before and less than 1 0r 2
with pl as (
    select p.gsis_id, ps.player_id , p.status, ps.season from players p
      full outer join player_stats ps on p.gsis_id = ps.player_id )
select player_id, season, count(*) from pl
where gsis_id is null and player_id is not null
group by season, player_id
;


--
-- IMPUTATION
--

-- just cosmetic - as long as there are just a few
--   these will never really join to anything and we've already made sure there a very few
delete from players where gsis_id is null;

-- fill missing status in players with a 'NOT-STATED' field - we won't really be predicting on this, so its ok
update players set status = 'NONE' where status is null;



--
--
--




--
-- REFERENCE
--

-- 500 players with no stats: no problem, leave it alone
with pl as (
    select p.gsis_id, ps.player_id , p.status as status, p.season from players p
                                                                           full outer join player_stats ps on p.gsis_id = ps.player_id  )
select pl.status, count(*) from pl where gsis_id is not null and player_id is  null
group by pl.status
;




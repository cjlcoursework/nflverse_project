---
--- Player stats
---
--- players is just information primarily, so player_stats is key, and they look good
---  we'll just backfill or delete
--- In an automated data stream
---

--
-- PREPROCESS
--
-- player_id,player_display_name
-- for each, replace the position and position_group where they are null
[( '00-0027567', 'Steve Maneri' , 'TE'),
 '00-0028543', 'Jeff Maehl'  , 'WR'),
 '00-0025569', 'Adam Hayward'  ,'LB'),
 '00-0029675', 'Trent Richardson' ,'RB')]



--
-- GATES
--

-- season should never be null
select count(*) from player_stats where season is null;

-- week should never be null
select count(*) from player_stats where week is null;

--- player_id should not be null
select * from player_stats where player_id is null;

select count(*) from player_stats where season_type is null;  -- should be zero

-- position
select player_id, season,  player_display_name, count(*) from player_stats where position is null group by season, player_id, player_display_name;




--
-- IMPUTATION
--

-- no nulls

-- season_type
select season_type, count(*) from player_stats group by season_type;

select count(*) from player_stats where season_type is null;  -- should be zero - or maybe very low - 1 or 2



-- cross pollinate
-- e.g.
-- set player_name = player_display_name where player name is null and player_display_name is not null
-- set player_display_name = player_name where player_display_name  is null and player_name is not null

-- player_name vs player_display_name
-- position vs position_group


-- impute season_type - todo - we should be able to tell with the 'week' field but it's not a critical field
update player_stats set season_type = 'REG' where season_type is null and week <= 18;  -- should be zero
update player_stats set season_type = 'POST' where season_type is null and week > 18;  -- should be zero





--- players is just information primarily, so player_stats is key, and they look good
---  we'll just backfill or delete
--- In an automated data stream
-- 69 players with null status
select count(*) from players where status is null;
--     fill missing status in players with a 'NOT-STATED' field - we won't really be predicting on this, so its ok
update players set status = 'NONE' where status is null;

-- 500 players with no stats: no problem, except:
with pl as (
    select p.gsis_id, ps.player_id , p.status as status, p.season from players p
                                                                           full outer join player_stats ps on p.gsis_id = ps.player_id  )
select pl.status, count(*) from pl where gsis_id is not null and player_id is  null
group by pl.status order by count(*) desc
;

-- 2 players with null gsis_id is null, and of course there is no join on player_stats
-- one is CUT, so we can backfill with a bogus gsis_id - just for completeness
-- the other has both null gsis_id and status - now filled with 'NOT-STATED'
-- so backfill '00-000000<index>' into gsis_id
with pl as (
    select p.gsis_id, ps.player_id , p.status, p.index from players p
    full outer join player_stats ps on p.gsis_id = ps.player_id )
select * from pl where  gsis_id is null and player_id is null
;
update players set gsis_id = '99-000'||index where gsis_id is null;




-- player_stats without player records: one stats with no player: 00-0005532
--  can delete this 1999 record from player_stats or leave it - it won't be used in any real calculations and it will drop out of some joins
with pl as (
    select p.gsis_id, ps.player_id , p.status, p.season from players p
    full outer join player_stats ps on p.gsis_id = ps.player_id )
select * from pl where  gsis_id is null and player_id is not null
;



--- FINAL check --
with pl as (
    select p.gsis_id, ps.player_id , p.status, p.season from players p
    full outer join player_stats ps on p.gsis_id = ps.player_id )
select * from pl where  gsis_id is null
;

select * from player_stats where player_id is null;



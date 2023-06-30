--
--
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



select S.index, count(*) from player_stats S group by S.index  having count(*) > 1;


select season, sum(carries) as carries, sum(rushing_yards), sum(rushing_yards)/sum(carries) as yards_per_carry  from player_stats
where player_id = '00-0033293'
group by season
order by season desc;




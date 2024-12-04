##///scheduled queries that run daily in Phoenix///
  
###retrieve campaign/volunteer names/IDs + each individual canvass attempt 
SELECT analytics_myv_van_id,
  date_canvassed,
  datetime_canvassed,
  canvassed_by_user_id,
  u.full_name,
  pu.public_user_name,
  COALESCE(u.full_name, pu.public_user_name) as username_coalesced,
  contact_type_id,
  contact_type_name,
  result_id,
  result_name,
  call,
  call_attempt,
  walk,
  walk_attempt,
  field_contact,
  field_contact_attempt,
  successful_contact,
  state_house_district,
  p.voting_address_id,
  p.voting_street_address,
  p.voting_street_address_2,
  p.voting_zip,
  p.voting_zip4,
  pr.precinct_code
 FROM `demscopaschal24.vansync_derived.contacts_myv` as m    
 LEFT JOIN `demscopaschal24.vansync_derived.users_access` as u
   ON m.canvassed_by_user_id = u.user_id
 LEFT JOIN `demscopaschal24.vansync.public_users` as pu   
   ON m.canvassed_by_user_id = pu.public_user_id
 LEFT JOIN `demscopaschal24.analytics_co_split.person` as p
   ON m.analytics_myv_van_id = p.myv_van_id
 LEFT JOIN `demscopaschal24.voter_file_co_split.precinct` as pr
  ON p.dnc_precinct_id = pr.dnc_precinct_id
 WHERE state_house_district = '018' OR state_house_district IS NULL
 ORDER BY 3 DESC;


###aggregates canvassing stats for each precinct in CO HD 18
WITH canvassing AS (
  SELECT analytics_myv_van_id,
    date_canvassed,
    datetime_canvassed,
    canvassed_by_user_id,
    u.full_name,
    pu.public_user_name,
    COALESCE(u.full_name, pu.public_user_name) as username_coalesced,
    contact_type_id,
    contact_type_name,
    result_id,
    result_name,
    call,
    call_attempt,
    walk,
    walk_attempt,
    field_contact,
    field_contact_attempt,
    successful_contact,
    state_house_district,
    p.voting_address_id,
    p.voting_street_address,
    p.voting_street_address_2,
    p.voting_zip,
    p.voting_zip4,
    pr.precinct_code
   FROM `demscopaschal24.vansync_derived.contacts_myv` as m    
   LEFT JOIN `demscopaschal24.vansync_derived.users_access` as u
     ON m.canvassed_by_user_id = u.user_id
   LEFT JOIN `demscopaschal24.vansync.public_users` as pu   
     ON m.canvassed_by_user_id = pu.public_user_id
   LEFT JOIN `demscopaschal24.analytics_co_split.person` as p
     ON m.analytics_myv_van_id = p.myv_van_id
   LEFT JOIN `demscopaschal24.voter_file_co_split.precinct` as pr
    ON p.dnc_precinct_id = pr.dnc_precinct_id
   WHERE state_house_district = '018' OR state_house_district IS NULL
   ORDER BY 3 DESC
),
  
precinct_stats AS (
SELECT precinct_code,
  SUM(walk) as canvassed,
  SUM(walk_attempt) as doors_knocked,
  SUM(field_contact_attempt) as total_attempts,
 FROM canvassing
 GROUP BY 1
)
  
SELECT precinct_stats.precinct_code,
  canvassed,
  doors_knocked,
  total_attempts,
  CASE 
    WHEN total_attempts != 0 THEN ROUND((canvassed/total_attempts)*100,1)
    ELSE total_attempts 
  END AS overall_contact_rate,
  CASE 
    WHEN doors_knocked != 0 THEN ROUND((canvassed/doors_knocked)*100,1)
    ELSE doors_knocked  
  END AS knock_contact_rate
 FROM precinct_stats
 ORDER BY 2 DESC;



###calculate cavnass stats per volunteer
WITH canvassing AS (
  SELECT analytics_myv_van_id,
    date_canvassed,
    datetime_canvassed,
    canvassed_by_user_id,
    u.full_name,
    pu.public_user_name,
    COALESCE(u.full_name, pu.public_user_name) as username_coalesced,
    contact_type_id,
    contact_type_name,
    result_id,
    result_name,
    call,
    call_attempt,
    walk,
    walk_attempt,
    field_contact,
    field_contact_attempt,
    successful_contact,
    state_house_district,
    p.voting_address_id,
    p.voting_street_address,
    p.voting_street_address_2,
    p.voting_zip,
    p.voting_zip4,
    pr.precinct_code
   FROM `demscopaschal24.vansync_derived.contacts_myv` as m    
   LEFT JOIN `demscopaschal24.vansync_derived.users_access` as u
     ON m.canvassed_by_user_id = u.user_id
   LEFT JOIN `demscopaschal24.vansync.public_users` as pu   
     ON m.canvassed_by_user_id = pu.public_user_id
   LEFT JOIN `demscopaschal24.analytics_co_split.person` as p
     ON m.analytics_myv_van_id = p.myv_van_id
   LEFT JOIN `demscopaschal24.voter_file_co_split.precinct` as pr
    ON p.dnc_precinct_id = pr.dnc_precinct_id
   WHERE state_house_district = '018' OR state_house_district IS NULL
   ORDER BY 3 DESC
),
  
field_stats AS (
SELECT username_coalesced,
  SUM(walk) as canvassed,
  SUM(walk_attempt) as doors_knocked,
  SUM(field_contact_attempt) as total_attempts,
 FROM canvassing
 GROUP BY 1
),

daily_hours AS (
SELECT date_canvassed,
  username_coalesced,
  ROUND(timestamp_diff(MAX(datetime_canvassed), MIN(datetime_canvassed), minute)/60,2) AS hours_worked
FROM canvassing
GROUP BY 1, 2
ORDER BY 1, 2
),

all_hours AS (
 SELECT username_coalesced,
  ROUND(SUM(hours_worked),2) as total_hours
 FROM daily_hours
 GROUP BY 1
)

SELECT field_stats.username_coalesced,
  total_hours,
  canvassed,
  doors_knocked,
  total_attempts,
  CASE 
    WHEN total_attempts != 0 THEN ROUND((canvassed/total_attempts)*100,1)
    ELSE total_attempts 
  END AS overall_contact_rate,
  CASE 
    WHEN doors_knocked != 0 THEN ROUND((canvassed/doors_knocked)*100,1)
    ELSE doors_knocked  
  END AS knock_contact_rate,
  CASE 
    WHEN doors_knocked != 0 THEN ROUND((doors_knocked/total_hours),1)
    ELSE doors_knocked
  END AS doors_per_hr
 FROM field_stats
 LEFT JOIN all_hours
   ON field_stats.username_coalesced = all_hours.username_coalesced
 ORDER BY 2 DESC;

##///end scheduled queries##



###calculate total number of voters per party in each precinct
#group by party and precinct to calculate voters per precinct
SELECT COUNT(pe.person_id) as voter_count,
  pr.precinct_code,
  pe.party_name_dnc
 FROM `demscopaschal24.analytics_co_split.person` as pe
 LEFT JOIN `demscopaschal24.voter_file_co_split.precinct` as pr
  ON pe.dnc_precinct_id = pr.dnc_precinct_id
 WHERE pr.is_active = true AND pe.state_house_district_latest = '018'
 GROUP BY 2, 3
 ORDER BY 2 ASC;


###calculate precinct leans in 2022 State House Race
#count total votes by precinct
WITH precinct_sum_votes AS (
SELECT p.PRECINCT,
  STATENUM,
  SUM(votes) as total_votes
 FROM `demscopaschal24.sbx_gersona.district_18_precincts` as p
 JOIN `demscopaschal24.sbx_gersona.g_2022` as g
   ON p.STATENUM = g.precinct
WHERE REP = 18 AND office = 'State House' AND party IN ('DEM', 'REP') 
GROUP BY 1, 2
),

#determine winner of each precinct
vote_shares AS (
  SELECT p.PRECINCT, 
    g.office,
    g.party,
    g.votes,
    RANK() OVER (PARTITION BY g.precinct ORDER BY g.votes DESC) as precinct_rank,
    p.total_votes,
    (g.votes/p.total_votes)*100 as winning_share,
    (1-(g.votes/p.total_votes))*100 as losing_share,
    #calculate margin of victory 
    ROUND(((g.votes/p.total_votes) -(1-(g.votes/p.total_votes)))*100,2) as diff
  FROM `demscopaschal24.sbx_gersona.g_2022` as g
  JOIN precinct_sum_votes as p
    ON g.precinct = p.STATENUM
  WHERE office = 'State House' AND party IN ('DEM', 'REP') AND votes != 0
  #return only winning party in precinct
  QUALIFY precinct_rank = 1
)

#return winning party of each precinct and cleaned margin for use in DEM dashboard
SELECT precinct,
  office,
  party,
  #return margin of victory/loss from perspective of DEM for conditional formatting in dashboard
  CASE 
    WHEN party = 'DEM' THEN diff
    ELSE -diff
  END AS diff,
  #return margin of victory/loss from perspective of DEM for tooltips in dashboard
  CASE 
    WHEN party = 'DEM' THEN CONCAT('D+', diff)
    ELSE CONCAT('R+', diff) 
  END AS lean
 FROM vote_shares
 ORDER BY precinct ASC;

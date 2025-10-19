import awswrangler as wr
import pandas as pd
import boto3

def getMacroData(puuid):
    query = f"""
WITH per_game AS (
SELECT
      puuid,
      riotidgamename,
      COALESCE(NULLIF(teamposition, ''), NULLIF(role, '')) AS role_std,
      teamid,
      timeplayed,

      (COALESCE(baronkills,0) 
       + COALESCE(dragonkills,0) 
       + COALESCE(challenges.riftHeraldTakedowns,0))                                    AS epic_obj_takedowns,

      CASE 
        WHEN (COALESCE(challenges.teamBaronKills,0)
           +  COALESCE(challenges.teamRiftHeraldKills,0)
           +  COALESCE(challenges.teamElderDragonKills,0)) > 0
        THEN CAST(
          (COALESCE(baronkills,0)
         +  COALESCE(dragonkills,0)
         +  COALESCE(challenges.riftHeraldTakedowns,0)) AS DOUBLE)
         / NULLIF(
            CAST(COALESCE(challenges.teamBaronKills,0)
           + COALESCE(challenges.teamRiftHeraldKills,0)
           + COALESCE(challenges.teamElderDragonKills,0) AS DOUBLE), 0)
        ELSE NULL 
      END                                                                                AS epic_obj_share,

      -- Damage to map objectives per minute
      (CAST(COALESCE(damagedealttoobjectives,0) AS DOUBLE) 
       / NULLIF(CAST(timeplayed AS DOUBLE),0)) * 60                                      AS obj_dmg_per_min,

      COALESCE(challenges.junglerTakedownsNearDamagedEpicMonster,0)                      AS fights_near_objectives,
      COALESCE(turrettakedowns,0)                                                        AS turret_takedowns,
      (CAST(COALESCE(damagedealttoturrets,0) AS DOUBLE)
       / NULLIF(CAST(timeplayed AS DOUBLE),0)) * 60                                      AS turret_dpm,

      COALESCE(challenges.visionScorePerMinute,
               (CAST(COALESCE(visionscore,0) AS DOUBLE)
                / NULLIF(CAST(timeplayed AS DOUBLE),0)) * 60)                            AS vision_spm,
      (CAST(COALESCE(wardsplaced,0) + COALESCE(wardskilled,0) AS DOUBLE)
       / NULLIF(CAST(timeplayed AS DOUBLE),0)) * 60                                      AS ward_actions_per_min,

      COALESCE(challenges.killParticipation,0.0)                                         AS kill_participation,
      COALESCE(challenges.pickKillWithAlly,0)                                            AS coordinated_kills,
      COALESCE(challenges.teleportTakedowns,0)                                           AS tp_takedowns,

      COALESCE(challenges.getTakedownsInAllLanesEarlyJungleAsLaner,0)                    AS early_crosslane_takes
  FROM {puuid}
  WHERE timeplayed >= 300               
),

per_player AS (
  SELECT
      puuid,
      any_value(riotidgamename)                                   AS player_name,
      any_value(role_std)                                       AS role_std,
      COUNT(*)                                                  AS games,

      AVG(epic_obj_takedowns)                                   AS avg_epic_obj_takes,
      AVG(epic_obj_share)                                       AS avg_epic_obj_share,
      AVG(obj_dmg_per_min)                                      AS avg_obj_dmg_per_min,
      AVG(fights_near_objectives)                               AS avg_obj_fight_takes,

      AVG(turret_takedowns)                                     AS avg_turret_takedowns,
      AVG(turret_dpm)                                           AS avg_turret_dpm,

      AVG(vision_spm)                                           AS avg_vision_spm,
      AVG(ward_actions_per_min)                                 AS avg_ward_apm,

      AVG(kill_participation)                                   AS avg_kp,
      AVG(coordinated_kills)                                    AS avg_coordinated_kills,
      AVG(tp_takedowns)                                         AS avg_tp_takedowns,
      AVG(early_crosslane_takes)                                AS avg_early_crosslane_takes
  FROM per_game
  GROUP BY puuid
),

scored AS (
  SELECT
      *,
      (
        -- Objective presence matters a lot
          COALESCE(avg_epic_obj_share, 0) * 40
        + LEAST(COALESCE(avg_epic_obj_takes, 0), 2) * 5        

        + LEAST(COALESCE(avg_vision_spm, 0), 2.5) * 10          
        + LEAST(COALESCE(avg_ward_apm, 0), 2.0) * 5

        + LEAST(COALESCE(avg_turret_dpm, 0) / 250.0, 2.0) * 8    -- 250 dpm ≈ healthy split/siege
        + LEAST(COALESCE(avg_obj_dmg_per_min, 0) / 400.0, 2.0) * 8

        + COALESCE(avg_kp, 0) * 12                               
        + LEAST(COALESCE(avg_coordinated_kills, 0), 3) * 2
        + LEAST(COALESCE(avg_tp_takedowns, 0), 2) * 3
        + LEAST(COALESCE(avg_early_crosslane_takes, 0), 2) * 3
      ) AS macro_index
  FROM per_player
)

SELECT
  puuid,
  player_name,
  role_std,
  games,
  ROUND(avg_epic_obj_share, 3)        AS epic_obj_share,
  ROUND(avg_epic_obj_takes, 2)        AS epic_obj_takes,
  ROUND(avg_obj_dmg_per_min, 1)       AS obj_dmg_per_min,
  ROUND(avg_obj_fight_takes, 2)       AS fights_near_objectives,
  ROUND(avg_turret_takedowns, 2)      AS turret_takedowns,
  ROUND(avg_turret_dpm, 1)            AS turret_dpm,
  ROUND(avg_vision_spm, 2)            AS vision_spm,
  ROUND(avg_ward_apm, 2)              AS ward_actions_per_min,
  ROUND(avg_kp, 2)                    AS kill_participation,
  ROUND(avg_coordinated_kills, 2)     AS coordinated_kills,
  ROUND(avg_tp_takedowns, 2)          AS tp_takedowns,
  ROUND(avg_early_crosslane_takes,2)  AS early_crosslane_takes,
  ROUND(macro_index, 1)               AS macro_index
FROM scored
WHERE games >= 5                        
ORDER BY macro_index DESC, games DESC
LIMIT 40;
"""
    df = wr.athena.read_sql_query(
        sql=query,
        database="riftrewindinput",
        boto3_session=boto3.Session(region_name='us-west-2')
    )
    json_string = df.to_json(orient='records')
    return json_string

def generateQuantitativeStatsGraphData(puuid):
    query = f"""
WITH base AS (
  SELECT
      puuid,
      timeplayed,
      CAST(kills AS DOUBLE)                       AS kills,
      CAST(deaths AS DOUBLE)                      AS deaths,
      CAST(assists AS DOUBLE)                     AS assists,
      CAST(visionscore AS DOUBLE)                 AS vision_score,
      CAST(totaldamagedealttochampions AS DOUBLE) AS dmg_to_champs,
      CAST(goldearned AS DOUBLE)                  AS gold,
      (CAST(kills AS DOUBLE) + CAST(assists AS DOUBLE)) / GREATEST(CAST(deaths AS DOUBLE), 1) AS kda
  FROM {puuid}
),
indexed AS (
  SELECT
    puuid,
    timeplayed,
    kills, deaths, assists, vision_score, dmg_to_champs, gold, kda,
    ROW_NUMBER() OVER (PARTITION BY puuid ORDER BY timeplayed) AS match_index
  FROM base
),
long_metrics AS (
  SELECT
    puuid,
    match_index,
    timeplayed,
    kv.metric,
    kv.value
  FROM indexed
  CROSS JOIN UNNEST(
    MAP(
      ARRAY['kills','deaths','assists','kda','vision_score','dmg_to_champs','gold'],
      ARRAY[kills,  deaths,  assists,  kda,  vision_score,   dmg_to_champs,  gold]
    )
  ) AS kv (metric, value)
),
with_trends AS (
  SELECT
    puuid,
    match_index,
    metric,
    value,
    LAG(value) OVER w                                                   AS prev_value,
    value - LAG(value) OVER w                                           AS delta,
    CASE
      WHEN LAG(value) OVER w IS NULL OR LAG(value) OVER w = 0
        THEN NULL
      ELSE (value - LAG(value) OVER w) / ABS(LAG(value) OVER w)
    END                                                                 AS pct_change,
    AVG(value) OVER (w ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)        AS rolling_avg_5,
    AVG(value) OVER (w ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)        AS rolling_avg_10
  FROM long_metrics
  WINDOW w AS (PARTITION BY puuid, metric ORDER BY match_index)
)
SELECT
  puuid,
  match_index,
  metric,
  value,
  delta,
  pct_change,
  rolling_avg_5,
  rolling_avg_10
FROM with_trends
ORDER BY puuid, metric, match_index;
"""
    df = wr.athena.read_sql_query(
        sql=query,
        database="riftrewindinput",
        boto3_session=boto3.Session(region_name='us-west-2')
    )
    json_string = df.to_json(orient='records')
    return json_string

def generateQualitativeStatsGraphData(puuid):
    query = f"""
WITH base AS (
  SELECT
    puuid,
    timeplayed,  -- seconds
    COALESCE(allinpings,0) + COALESCE(assistmepings,0) + COALESCE(basicpings,0) +
    COALESCE(commandpings,0) + COALESCE(dangerpings,0) + COALESCE(enemymissingpings,0) +
    COALESCE(enemyvisionpings,0) + COALESCE(getbackpings,0) + COALESCE(holdpings,0) +
    COALESCE(onmywaypings,0) + COALESCE(pushpings,0) + COALESCE(retreatpings,0) +
    COALESCE(visionclearedpings,0) + COALESCE(needvisionpings,0)                   AS ping_total,

    COALESCE(wardsplaced,0) + COALESCE(wardskilled,0) + COALESCE(detectorwardsplaced,0) +
    COALESCE(sightwardsboughtingame,0) + COALESCE(visionwardsboughtingame,0) +
    COALESCE(challenges.controlWardsPlaced,0)                                       AS ward_actions,

    COALESCE(assists,0)                                                             AS assists,

    COALESCE(challenges.visionScorePerMinute,
             CASE WHEN timeplayed > 0
                  THEN CAST(visionscore AS DOUBLE) / (CAST(timeplayed AS DOUBLE)/60.0)
                  ELSE NULL END)                                                    AS vspm,

    COALESCE(dragonkills,0) + COALESCE(baronkills,0) + COALESCE(challenges.riftHeraldTakedowns,0) AS big_objectives,
    COALESCE(objectivesstolen,0) + 0.5*COALESCE(objectivesstolenassists,0)                         AS objective_steals,

    COALESCE(turretkills,0) + COALESCE(turrettakedowns,0)                                          AS turret_events,
    CAST(COALESCE(damagedealttoturrets,0) AS DOUBLE)                                               AS turret_damage,

    COALESCE(totalenemyjungleminionskilled,0) + COALESCE(challenges.scuttleCrabKills,0)            AS neutral_ctrl,

    CAST(COALESCE(challenges.killParticipation, NULL) AS DOUBLE)                                   AS kp
  FROM {puuid}
),
indexed AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY puuid ORDER BY timeplayed) AS totaldamagedealttochampions, ,
    NULLIF(CAST(timeplayed AS DOUBLE)/600.0, 0.0)               AS per10_den
  FROM base b
),
rates AS (
  SELECT
    puuid,
    match_index,
    timeplayed,
    (CAST(ping_total AS DOUBLE)   / per10_den)                               AS comm_pings_per10,
    (CAST(ward_actions AS DOUBLE) / per10_den)                               AS comm_wards_per10,
    (CAST(assists AS DOUBLE)      / per10_den)                               AS comm_assists_per10,
    vspm                                                                  AS comm_vspm,
    (CAST(big_objectives AS DOUBLE)     / per10_den)                         AS macro_bigobj_per10,
    (CAST(objective_steals AS DOUBLE)   / per10_den)                         AS macro_steals_per10,
    (CAST(turret_events AS DOUBLE)      / per10_den)                         AS macro_turret_events_per10,
    (turret_damage / 5000.0)        / per10_den                              AS macro_turret_dmg_scaled_per10,
    (CAST(neutral_ctrl AS DOUBLE)        / per10_den)                         AS macro_neutral_per10,
    kp                                                                       AS macro_kp
  FROM indexed
),
stats AS (
  SELECT
    puuid,
    AVG(comm_pings_per10)  AS mu_comm_pings,   STDDEV_SAMP(comm_pings_per10)  AS sd_comm_pings,
    AVG(comm_wards_per10)  AS mu_comm_wards,   STDDEV_SAMP(comm_wards_per10)  AS sd_comm_wards,
    AVG(comm_assists_per10)AS mu_comm_assists, STDDEV_SAMP(comm_assists_per10)AS sd_comm_assists,
    AVG(comm_vspm)         AS mu_comm_vspm,    STDDEV_SAMP(comm_vspm)         AS sd_comm_vspm,
    AVG(macro_bigobj_per10)        AS mu_macro_bigobj,   STDDEV_SAMP(macro_bigobj_per10)        AS sd_macro_bigobj,
    AVG(macro_steals_per10)        AS mu_macro_steals,   STDDEV_SAMP(macro_steals_per10)        AS sd_macro_steals,
    AVG(macro_turret_events_per10) AS mu_macro_turret_e, STDDEV_SAMP(macro_turret_events_per10) AS sd_macro_turret_e,
    AVG(macro_turret_dmg_scaled_per10) AS mu_macro_tdmg, STDDEV_SAMP(macro_turret_dmg_scaled_per10) AS sd_macro_tdmg,
    AVG(macro_neutral_per10)       AS mu_macro_neutral,  STDDEV_SAMP(macro_neutral_per10)       AS sd_macro_neutral,
    AVG(macro_kp)                  AS mu_macro_kp,       STDDEV_SAMP(macro_kp)                  AS sd_macro_kp
  FROM rates
  GROUP BY puuid
),
scored AS (
  SELECT
    r.puuid,
    r.match_index,
    r.timeplayed,

    CASE WHEN s.sd_comm_pings   > 0 THEN (r.comm_pings_per10   - s.mu_comm_pings)   / s.sd_comm_pings   ELSE 0 END AS z_comm_pings,
    CASE WHEN s.sd_comm_wards   > 0 THEN (r.comm_wards_per10   - s.mu_comm_wards)   / s.sd_comm_wards   ELSE 0 END AS z_comm_wards,
    CASE WHEN s.sd_comm_assists > 0 THEN (r.comm_assists_per10 - s.mu_comm_assists) / s.sd_comm_assists ELSE 0 END AS z_comm_assists,
    CASE WHEN s.sd_comm_vspm    > 0 THEN (r.comm_vspm          - s.mu_comm_vspm)    / s.sd_comm_vspm    ELSE 0 END AS z_comm_vspm,

    CASE WHEN s.sd_macro_bigobj > 0 THEN (r.macro_bigobj_per10 - s.mu_macro_bigobj) / s.sd_macro_bigobj ELSE 0 END AS z_macro_bigobj,
    CASE WHEN s.sd_macro_steals > 0 THEN (r.macro_steals_per10 - s.mu_macro_steals) / s.sd_macro_steals ELSE 0 END AS z_macro_steals,
    CASE WHEN s.sd_macro_turret_e > 0 THEN (r.macro_turret_events_per10 - s.mu_macro_turret_e) / s.sd_macro_turret_e ELSE 0 END AS z_macro_turret_e,
    CASE WHEN s.sd_macro_tdmg   > 0 THEN (r.macro_turret_dmg_scaled_per10 - s.mu_macro_tdmg) / s.sd_macro_tdmg ELSE 0 END AS z_macro_tdmg,
    CASE WHEN s.sd_macro_neutral> 0 THEN (r.macro_neutral_per10 - s.mu_macro_neutral) / s.sd_macro_neutral ELSE 0 END AS z_macro_neutral,
    CASE WHEN s.sd_macro_kp     > 0 THEN (r.macro_kp - s.mu_macro_kp) / s.sd_macro_kp ELSE 0 END AS z_macro_kp
  FROM rates r
  JOIN stats s
    ON r.puuid = s.puuid
),
final AS (
  SELECT
    puuid,
    match_index,
    ((z_comm_pings + z_comm_wards + z_comm_assists + z_comm_vspm) / 4.0)                          AS communication_score,
    ((z_macro_bigobj + z_macro_steals + z_macro_turret_e + z_macro_tdmg + z_macro_neutral + z_macro_kp) / 6.0) AS macro_score
  FROM scored
)
SELECT
  puuid,
  match_index,
  communication_score,
  macro_score,
  LAG(communication_score) OVER (PARTITION BY puuid ORDER BY match_index)                                   AS prev_comm_score,
  communication_score - LAG(communication_score) OVER (PARTITION BY puuid ORDER BY match_index)             AS delta_comm,
  AVG(communication_score) OVER (PARTITION BY puuid ORDER BY match_index ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS comm_rolling_avg_5,
  LAG(macro_score) OVER (PARTITION BY puuid ORDER BY match_index)                                            AS prev_macro_score,
  macro_score - LAG(macro_score) OVER (PARTITION BY puuid ORDER BY match_index)                              AS delta_macro,
  AVG(macro_score) OVER (PARTITION BY puuid ORDER BY match_index ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)   AS macro_rolling_avg_5
FROM final
ORDER BY puuid, match_index;
"""
    df = wr.athena.read_sql_query(
        sql=query,
        database="riftrewindinput",
        boto3_session=boto3.Session(region_name='us-west-2')
    )
    json_string = df.to_json(orient='records')
    return json_string


#REMEMBER PUT DATA INTO LLM TO ANALYSE AND TALK ABOUT TRENDS AND WHERE THEY CAN IMPROVE I WILL RETURN THE RAW DATA TOO TO THE LLM
#get the gold data over too and graph with scatter plot
#explain composite score of a bunch of things and feed into LLM
#remember to get the raw data too so players know where they can get better with improving macro

print(getMacroData("jzdg2rwr6k16dsjfalqjeixnhaa_yyffhr0xdpwqbzqieai2rpb4npjpd2zw_iibav31xmrtrz4p6g"))


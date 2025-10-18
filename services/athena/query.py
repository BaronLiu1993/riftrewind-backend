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

      -- Fights/picks near objectives (good macro timing)
      COALESCE(challenges.junglerTakedownsNearDamagedEpicMonster,0)                      AS fights_near_objectives,

      -- === Turret pressure ===
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


print(getMacroData("jzdg2rwr6k16dsjfalqjeixnhaa_yyffhr0xdpwqbzqieai2rpb4npjpd2zw_iibav31xmrtrz4p6g"))


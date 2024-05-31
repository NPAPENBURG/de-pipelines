INSERT INTO valorant_player_data (
    match_id, player, map_name, game_mode, start_time, season, region, cluster,
    player_team, player_level, character_name, tier, score, kills, deaths, assists,
    shots_head, shots_body, shots_leg, damage_made, damage_received,
    team_red_score, team_blue_score
)
SELECT
    temp.match_id, temp.player, temp.map_name, temp.game_mode, temp.start_time, temp.season, temp.region, temp.cluster,
    temp.player_team, temp.player_level, temp.character_name, temp.tier, temp.score, temp.kills, temp.deaths, temp.assists,
    temp.shots_head, temp.shots_body, temp.shots_leg, temp.damage_made, temp.damage_received,
    temp.team_red_score, temp.team_blue_score
FROM
    temp_valorant_player_data temp
LEFT JOIN
    valorant_player_data main
ON
    temp.match_id = main.match_id AND
    temp.player = main.player
WHERE
    main.match_id IS NULL;

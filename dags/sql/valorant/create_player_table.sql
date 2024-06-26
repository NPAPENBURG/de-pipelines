CREATE TABLE IF NOT EXISTS valorant_player_data (
    match_id VARCHAR(150),
    player VARCHAR(50),
    map_name VARCHAR(50),
    game_mode VARCHAR(50),
    start_time TIMESTAMP,
    season VARCHAR(50),
    region VARCHAR(50),
    cluster VARCHAR(50),
    player_team VARCHAR(50),
    player_level INT,
    character_name VARCHAR(50),
    tier INT,
    score INT,
    kills INT,
    deaths INT,
    assists INT,
    shots_head INT,
    shots_body INT,
    shots_leg INT,
    damage_made INT,
    damage_received INT,
    team_red_score INT,
    team_blue_score INT
);
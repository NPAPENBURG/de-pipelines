import json
import logging


def clean_valorant_data(ti, task_id):
    # Pull the XCom result from the specified task
    xcom_result = ti.xcom_pull(task_ids=task_id)

    # Log the XCom result for debugging purposes
    logging.info(f"XCom result: {xcom_result}")

    if xcom_result is None:
        raise ValueError(f"No data found from {task_id} task")

    data = json.loads(xcom_result)

    cleaned_data = []
    for match in data['data']:

        match_info = {
            'match_id': match['meta']['id'],
            'map_name': match['meta']['map']['name'],
            'game_mode': match['meta']['mode'],
            'start_time': match['meta']['started_at'],
            'season': match['meta']['season']['short'],
            'region': match['meta']['region'],
            'cluster': match['meta']['cluster'],
            'player_team': match['stats']['team'],
            'player_level': match['stats']['level'],
            'character_name': match['stats']['character']['name'],
            'tier': match['stats']['tier'],
            'score': match['stats']['score'],
            'kills': match['stats']['kills'],
            'deaths': match['stats']['deaths'],
            'assists': match['stats']['assists'],
            'shots_head': match['stats']['shots']['head'],
            'shots_body': match['stats']['shots']['body'],
            'shots_leg': match['stats']['shots']['leg'],
            'damage_made': match['stats']['damage']['made'],
            'damage_received': match['stats']['damage']['received'],
            'team_red_score': match['teams']['red'],
            'team_blue_score': match['teams']['blue'],
        }
        cleaned_data.append(match_info)

    # Optionally, you can push the cleaned data to XCom for further tasks
    ti.xcom_push(key='cleaned_data', value=cleaned_data)

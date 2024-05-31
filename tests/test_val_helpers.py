import pytest
import json
import logging
from unittest.mock import Mock
from tests.val_mock_data import mock_player_data
from dags.helpers.val_helpers import clean_valorant_data


class TestCleanValorantData:

    def test_clean_valorant_data(self):
        # Set up mock task instance
        mock_ti = Mock()
        mock_ti.xcom_pull.return_value = json.dumps(mock_player_data)

        # Call the function with the mock objects
        clean_valorant_data(ti=mock_ti, task_id='extract_AnimeWatcher_data')

        # Get the pushed cleaned data
        cleaned_data = mock_ti.xcom_push.call_args[1]['value']

        # Check that the cleaned data matches expected structure and values
        assert len(cleaned_data) == len(mock_player_data['data'])
        for match, cleaned_match in zip(mock_player_data['data'], cleaned_data):
            assert cleaned_match['match_id'] == match['meta']['id']
            assert cleaned_match['map_name'] == match['meta']['map']['name']
            assert cleaned_match['game_mode'] == match['meta']['mode']
            assert cleaned_match['start_time'] == match['meta']['started_at']
            assert cleaned_match['season'] == match['meta']['season']['short']
            assert cleaned_match['region'] == match['meta']['region']
            assert cleaned_match['cluster'] == match['meta']['cluster']
            assert cleaned_match['player_team'] == match['stats']['team']
            assert cleaned_match['player_level'] == match['stats']['level']
            assert cleaned_match['character_name'] == match['stats']['character']['name']
            assert cleaned_match['tier'] == match['stats']['tier']
            assert cleaned_match['score'] == match['stats']['score']
            assert cleaned_match['kills'] == match['stats']['kills']
            assert cleaned_match['deaths'] == match['stats']['deaths']
            assert cleaned_match['assists'] == match['stats']['assists']
            assert cleaned_match['shots_head'] == match['stats']['shots']['head']
            assert cleaned_match['shots_body'] == match['stats']['shots']['body']
            assert cleaned_match['shots_leg'] == match['stats']['shots']['leg']
            assert cleaned_match['damage_made'] == match['stats']['damage']['made']
            assert cleaned_match['damage_received'] == match['stats']['damage']['received']
            assert cleaned_match['team_red_score'] == match['teams']['red']
            assert cleaned_match['team_blue_score'] == match['teams']['blue']

        mock_ti.xcom_push.assert_called_once_with(key='cleaned_data', value=cleaned_data)

from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class SparkifyPlugin(AirflowPlugin):
    name = "sparkify_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.CreateTablesOperator
    ]
    helpers = [
        helpers.songplays_table_insert,
        helpers.users_table_insert,
        helpers.songs_table_insert,
        helpers.artists_table_insert,
        helpers.time_table_insert,
        helpers.staging_events_table_create,
        helpers.staging_songs_table_create,
        helpers.artists_table_create,
        helpers.songplays_table_create,
        helpers.songs_table_create,
        helpers.time_table_create,
        helpers.users_table_create
    ]

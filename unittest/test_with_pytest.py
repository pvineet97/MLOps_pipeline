##############################################################################
# Import the necessary modules
##############################################################################
from constants import *
import os
import sqlite3
import pandas as pd
import numpy as np
import unittest
from utils import *

###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

def connect_db(db_path,db_file_name):
    db_path = os.path.join(db_path,db_file_name)
    conn = sqlite3.connect(db_path)
    return conn

def test_load_data_into_db():
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_get_data()

    """
    load_data_into_db()
    conn = connect_db(DB_PATH,DB_FILE_NAME)
    output_df = pd.read_sql('select * from loaded_data',conn)
    conn.close()

    conn=connect_db(DB_PATH,UNIT_TEST_DB_FILE_NAME)
    expected_df = pd.read_sql('select * from loaded_data_test_case',conn)
    print(expected_df)
    conn.close()

    assert expected_df.equals(output_df)




    
    

###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier():
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_map_city_tier()

    """

    map_city_tier()
    conn = connect_db(DB_PATH,DB_FILE_NAME)
    output_df = pd.read_sql('select * from city_tier_mapped',conn)
    conn.close()

    conn=connect_db(DB_PATH,UNIT_TEST_DB_FILE_NAME)
    expected_df = pd.read_sql('select * from city_tier_mapped_test_case',conn)
    print(expected_df)
    conn.close()

    assert expected_df.equals(output_df)

 
    
###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """    
    map_categorical_vars()
    conn = connect_db(DB_PATH,DB_FILE_NAME)
    output_df = pd.read_sql('select * from categorical_variables_mapped',conn)
    conn.close()

    conn=connect_db(DB_PATH,UNIT_TEST_DB_FILE_NAME)
    expected_df = pd.read_sql('select * from categorical_variables_mapped_test_case',conn)
    print(expected_df)
    conn.close()

    assert expected_df.equals(output_df)

    

###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """ 
    interactions_mapping()
    conn = connect_db(DB_PATH,DB_FILE_NAME)
    output_df = pd.read_sql('select * from interaction_mapped',conn)
    conn.close()

    conn=connect_db(DB_PATH,UNIT_TEST_DB_FILE_NAME)
    expected_df = pd.read_sql('select * from interactions_mapped_test_case',conn)
    print(expected_df)
    conn.close()

    assert expected_df.equals(output_df)

test_load_data_into_db()
test_map_city_tier()   
test_map_categorical_vars()
test_interactions_mapping()


    
   

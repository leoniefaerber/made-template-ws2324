import unittest
import pipeline
import sqlite3
import pandas as pd

class TestPipeline(unittest.TestCase):

    def test_load_to_sqlite_file(self):
        data = pd.DataFrame([[1, 2, 3], [1, 2, 3]], columns=["a", "b", "c"])
        db_file = "load_test.sqlite"
        db_name = "load_test"
        test_file_path = "data/load_test.sqlite"

        pipeline.load_to_sqlite_file(data, db_file, db_name, [])
        conn = sqlite3.connect(test_file_path)
        result = pd.read_sql("SELECT * FROM load_test", conn)
        conn.close

        pd.testing.assert_frame_equal(result, data)
        

    def test_activity_pipeline(self):
        activity_data = pd.DataFrame(
            [
                [None, None, 'test', 'test', 'test', 'test',
                    'test', 'test', 'test', 0, 0.0, 'test'], 
                [None, None, 'test', 'test', 'test', 'test',
                    'test', 'test', 'test', 0, 0.0, 'test'],
                [None, None, 'test', 'test', 'test', 'test',
                'test', 'test', 'test', 0, 0.0, 'test'],
                [None, None, None, None, None, None,
                    None, None, None, 0, 0.0, None]
            ], 
            columns=['DATAFLOW', 'LAST UPDATE', 'freq', 'unit', 'duration', 'isced11', 'sex', 'age', 
                     'geo','TIME_PERIOD', 'OBS_VALUE', 'OBS_FLAG'])
        db_file = "activity_test.sqlite"
        db_name = "activity_test"
        activity_test_file_path = "data/activity_test.sqlite"

        pipeline.create_activity_table(activity_data, db_file, db_name)
        conn = sqlite3.connect(activity_test_file_path)
        result = pd.read_sql("SELECT * FROM activity_test", conn)
        conn.close

        self.assertEqual(len(result.columns), 11)   # should be 10; wrong assert to check CI
        self.assertEqual(len(result), 3)
    
    def test_mental_health_pipeline(self):
        mental_health_data = pd.DataFrame(
            [
                [None, None, 'test', 'test', 'test', 'test',
                    'test', 'test', 'test', 0, 0.0, 'test'], 
                [None, None, 'test', 'test', 'test', 'test',
                    'test', 'test', 'test', 0, 0.0, 'test'],
                [None, None, 'test', 'test', 'test', 'test',
                'test', 'test', 'test', 0, 0.0, 'test'],
                [None, None, None, None, None, None,
                    None, None, None, 0, 0.0, None]
            ], 
            columns=['DATAFLOW', 'LAST UPDATE', 'freq', 'unit', 'isced11', 'hlth_pb',
                    'sex', 'age', 'geo','TIME_PERIOD', 'OBS_VALUE', 'OBS_FLAG'])
        db_file = "mental_health_test.sqlite"
        db_name = "mental_health_test"
        mental_health_test_file_path = "data/mental_health_test.sqlite"

        pipeline.create_mental_health_table(mental_health_data, db_file, db_name)
        conn = sqlite3.connect(mental_health_test_file_path)
        result = pd.read_sql("SELECT * FROM mental_health_test", conn)
        conn.close

        self.assertEqual(len(result.columns), 10)
        self.assertEqual(len(result), 3)

    
    def test_general_health_pipeline(self):
        general_health_data = pd.DataFrame(
            [
                [None, None, 'test', 'test', 'test', 'test',
                    'test', 'test', 'test', 0, 0.0, 'test'], 
                [None, None, 'test', 'test', 'test', 'test',
                    'test', 'test', 'test', 0, 0.0, 'test'],
                [None, None, 'test', 'test', 'test', 'test',
                'test', 'test', 'test', 0, 0.0, 'test'],
                [None, None, None, None, None, None,
                    None, None, None, 0, 0.0, None]
            ], 
            columns=['DATAFLOW', 'LAST UPDATE', 'freq', 'unit', 'isced11', 'age',
                    'sex', 'levels', 'geo','TIME_PERIOD', 'OBS_VALUE', 'OBS_FLAG'])
        db_file = "general_health_test.sqlite"
        db_name = "general_health_test"
        general_health_test_file_path = "data/general_health_test.sqlite"

        pipeline.create_general_health_table(general_health_data, db_file, db_name)
        conn = sqlite3.connect(general_health_test_file_path)
        result = pd.read_sql("SELECT * FROM general_health_test", conn)
        conn.close

        self.assertEqual(len(result.columns), 10)
        self.assertEqual(len(result), 3)

if __name__ == '__main__':
    unittest.main()
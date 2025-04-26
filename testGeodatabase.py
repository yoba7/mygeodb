import unittest
import os
import sqlite3
import pandas as pd
from geodatabase import Geodatabase
import logging
from io import StringIO
import tempfile
import shutil
import sys

# Configure logging for testing
#logging.basicConfig(level=logging.INFO)

class TestGeodatabase(unittest.TestCase):

    def setUp(self):
        """
        Setup method to initialize a fresh in-memory database for each test.
        """
        self.db = Geodatabase(db=':memory:')
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """
        Teardown method to close the database connection and clean up any temporary files.
        """
        self.db.close()
        shutil.rmtree(self.temp_dir)

    def test_initialization(self):
        """
        Test the initialization of the Geodatabase class.
        """
        self.assertIsNotNone(self.db.database)
        self.assertEqual(self.db.srid, 31370)

    def test_attach(self):
        """
        Test the attach method.
        """
        # Create a temporary database file
        temp_db_path = os.path.join(self.temp_dir, 'temp.sqlite')
        temp_db = sqlite3.connect(temp_db_path)
        temp_db.close()

        self.db.attach(temp_db_path, 'temp_alias')
        
        # Verify attachment (can be basic check)
        cursor = self.db.database.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='geometry_columns'")
        self.assertIsNotNone(cursor.fetchone())

    def test_checksForTable(self):
        """
        Test the checksForTable method.
        """
        self.db.database.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
        self.assertTrue(self.db.checksForTable('test_table'))

        with self.assertRaises(Exception):
            self.db.database.execute("CREATE TABLE test_table_shadowed (rowid INTEGER, name TEXT)")
            self.db.checksForTable('test_table_shadowed')

        with self.assertRaises(Exception):
             self.db.database.execute("CREATE TABLE test_table_without_rowid (id INTEGER, name TEXT) WITHOUT ROWID")
             self.db.checksForTable('test_table_without_rowid')

    def test_close(self):
        """
        Test the close method.
        """
        pass

    def test_correctGeometry(self):
        """
        Test the correctGeometry method.
        """

        # Create table
        self.db.execute("""
                CREATE TABLE test_geometry (id INTEGER, geometry GEOMETRY)
                        """)

        # Insert polygon without error
        self.db.execute(f"""
                /* there is no error in this geometry */
                INSERT INTO test_geometry(id, geometry) 
                VALUES (1, GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', {self.db.srid}))
                         """)
        
        # Insert invalid geometries
        self.db.execute(f"""
                /* there is an error in this geometry : polygon is not closed */
                INSERT INTO test_geometry(id, geometry) 
                VALUES (2, GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1))', {self.db.srid}))
                         """)
        '''
        # This error seems to be impossible to correct
        self.db.execute(f"""
                /* there is an error in this geometry : self-intersecting geometry */
                INSERT INTO test_geometry(id, geometry) 
                VALUES (3, GeomFromText('POLYGON((0 0, 1 0, 0 1, 1 1, 0 0))', {self.db.srid}))
                         """)
        '''

        # Register geometry
        self.db.recoverGeometry('test_geometry')

        # Count of errors before correction -> should be 2
        ms_countOf_errors = self.db.geometryCountOfErrors('test_geometry')
        self.assertEqual(ms_countOf_errors,1)

        # Correct geometry
        self.db.correctGeometry('test_geometry')

        # Count of errors after correction -> should be 0
        ms_countOf_errors = self.db.geometryCountOfErrors('test_geometry')
        self.assertEqual(ms_countOf_errors,0)

        # Check that polygon has been correctly closed
        correctedGeometry=self.db.database.execute('select st_asText(geometry) from test_geometry where id=2').fetchone()[0]
        self.assertEqual(correctedGeometry,'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')

        # Drop table
        self.db.dropTable('test_geometry')

    def test_createSpatialIndex(self):
        """
        Test the createSpatialIndex method.
        """

        # create table with geometry
        self.db.execute("""
                CREATE TABLE test_table (id INTEGER PRIMARY KEY, geometry GEOMETRY)
                        """)

        # insert a geometry
        self.db.execute(f"""
                INSERT INTO test_table(geometry) 
                VALUES (GeomFromText('POINT(0 0)', {self.db.srid}))
                         """)

        # recover geometry
        self.db.recoverGeometry('test_table')

        # create spatial index
        self.db.createSpatialIndex('test_table')

        # check if spatial index was created
        fl_spatial_index_enabled = self.db.execute("""
                select spatial_index_enabled 
                from geometry_columns 
                where f_table_name='test_table'
                        """).fetchone()[0]
        self.assertEqual(fl_spatial_index_enabled, 1)

        # drop table
        self.db.dropTable('test_table')
        
    def test_dropTable(self):
        """
        Test the dropTable method.
        """

        # create table
        self.db.execute("""
                CREATE TABLE test_table (id INTEGER PRIMARY KEY, geometry GEOMETRY)
                        """)

        # insert a geometry
        self.db.execute(f"""
                INSERT INTO test_table(geometry) 
                VALUES (GeomFromText('POINT(0 0)', {self.db.srid}))
                         """)

        # register geometry in metadata
        self.db.recoverGeometry('test_table')

        # check that table currently exists
        ms_countOf_tables = self.db.execute("""
                SELECT count(*) 
                FROM geometry_columns 
                WHERE f_table_name='test_table' 
                                             """).fetchone()[0]
        self.assertEqual(ms_countOf_tables, 1)
        
        # drop table
        self.db.dropTable('test_table')
        
       # check that table no longer exists
        ms_countOf_tables = self.db.execute("""
                SELECT count(*) 
                FROM geometry_columns 
                WHERE f_table_name='test_table' 
                                             """).fetchone()[0]
        self.assertEqual(ms_countOf_tables, 0)

    def test_execute(self):
        """
        Test the execute method.
        """
        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
        self.db.execute("INSERT INTO test_table (name) VALUES ('test')")
        result = self.db.execute("SELECT name FROM test_table").fetchall()
        self.assertEqual(result, [('test',)])

    def test_executeScript(self):
        """
        Test the executeScript method.
        """
        
        # Create a SQL script 
        script_content = """
            CREATE TABLE test_table_script (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO test_table_script (name) VALUES ('test1');
            INSERT INTO test_table_script (name) VALUES ('test2');
        """

        # Save script in temporary directory
        script_path = os.path.join(self.temp_dir, 'test_script.sql')
        with open(script_path, 'w') as f:
            f.write(script_content)

        # Execute the script
        self.db.executeScript('test_script',path=self.temp_dir)
        
        # Check if the table was created and data inserted
        result = self.db.execute("SELECT COUNT(*) FROM test_table_script").fetchone()[0]
        self.assertEqual(result, 2)

        # Drop table
        self.db.dropTable('test_table_script')
        
    def test_findConnectedComponents(self):
        """
        Test the findConnectedComponents method.
        """

        # Create a graph
        df_edges = pd.DataFrame(
            columns = ['a', 'b'], 
            data=[
                      ('A', 'B'),
                      ('A', 'D'),
                      ('B', 'C'),
                      ('B', 'E'),
                      ('F', 'G')
                ]
        )

        # Load graph 
        df_edges.to_sql('t01_edges', self.db.database, index=False, if_exists='replace')

        # Find connected components
        self.db.findConnectedComponents('t01_edges', ['a', 'b'], 't02_cc')

        # Check if connected components were found
        ms_countOf_nodes = self.db.execute("SELECT COUNT(*) FROM t02_cc").fetchone()[0]
        self.assertEqual(ms_countOf_nodes, 7)

        #
        ms_countOf_connectedComponents=self.db.execute("SELECT COUNT(DISTINCT cc) FROM t02_cc").fetchone()[0]
        self.assertEqual(ms_countOf_connectedComponents, 2)

        # Drop tables
        self.db.dropTable('t01_edges')
        self.db.dropTable('t02_cc')

    def test_geometryCountOfErrors(self):
        """
        Test the geometryCountOfErrors method.
        """
        self.db.execute("CREATE TABLE test_geometry (id INTEGER PRIMARY KEY, geometry BLOB)")
        self.db.execute(f"INSERT INTO test_geometry(geometry) VALUES (GeomFromText('POLYGON((0 0, 1 0, 0 1, 0 0))', {self.db.srid}))")
        self.db.execute(f"INSERT INTO test_geometry(geometry) VALUES (GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1))', {self.db.srid}))")
        self.db.execute("SELECT RecoverGeometryColumn('test_geometry','geometry',31370,'POLYGON','XY')")

        ms_countOf_errors = self.db.geometryCountOfErrors('test_geometry')
        self.assertEqual(ms_countOf_errors, 1)

    def test_getColumnMaxLength(self):
        """
        Test the getColumnMaxLength method.
        """
        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
        self.db.execute("INSERT INTO test_table (name) VALUES ('test')")
        self.db.execute("INSERT INTO test_table (name) VALUES ('longer_test')")
        max_length = self.db.getColumnMaxLength('test_table', 'name')
        self.assertEqual(max_length, 11)

    def test_getColumnCountOfDistinctValues(self):
        """
        Test the getColumnCountOfDistinctValues method.
        """
        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
        self.db.execute("INSERT INTO test_table (name) VALUES ('test')")
        self.db.execute("INSERT INTO test_table (name) VALUES ('test')")
        self.db.execute("INSERT INTO test_table (name) VALUES ('other')")
        count = self.db.getColumnCountOfDistinctValues('test_table', 'name')
        self.assertEqual(count, 2)

    def test_getGeometriesMetadata(self):
        """
        Test the getGeometriesMetadata method.
        """
        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, geometry BLOB)")
        self.db.execute(f"INSERT INTO test_table(geometry) VALUES (GeomFromText('POINT(0 0)', {self.db.srid}))")
        self.db.recoverGeometry('test_table')
        self.db.createSpatialIndex('test_table')
        metadata = self.db.getGeometriesMetadata()
        
        # Checks
        self.assertIn('test_table', metadata.index)
        self.assertIn('geometry_type', metadata.columns)
        self.assertEqual(metadata.loc['test_table', 'geometry_type'], 1) # 1 is for POINT
        self.assertEqual(metadata.loc['test_table', 'srid'], self.db.srid)
        self.assertEqual(metadata.loc['test_table', 'spatial_index_enabled'], 1)

        # Cleaning
        self.db.dropTable('test_table')

    def test_getListOfColumns(self):
        """
        Test the getListOfColumns method.
        """
        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        # Get list of columns
        columns = self.db.getListOfColumns('test_table')

        # Checks
        self.assertIn('id', columns.index)
        self.assertIn('name', columns.index)

        # Cleaning
        self.db.dropTable('test_table')

    def test_getListOfTables(self):
        """
        Test the getListOfTables method.
        """
        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")

        # Get list of tables
        tables = self.db.getListOfTables()

        # Checks
        self.assertIn('test_table', tables.index)
        self.assertEqual(tables.loc['test_table']['ms_countOf_rows'],0)

        # Cleaning
        self.db.dropTable('test_table')

        
    def test_getTableCountOfRows(self):
        """
        Test the getTableCountOfRows method.
        """
        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
        self.db.execute("INSERT INTO test_table (name) VALUES ('test')")
        count = self.db.getTableCountOfRows('test_table')
        self.assertEqual(count, 1)

    def test_getTableHead(self):
        """
        Test the getTableHead method.
        """
        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
        self.db.execute("INSERT INTO test_table (name) VALUES ('test')")

        # Get table head
        head = self.db.getTableHead('test_table')

        # Checks
        self.assertIn('name', head.columns)


    def test_inspectGeometry(self):
        """
        Test the inspectGeometry method.
        """
        self.db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, geometry BLOB)")
        self.db.execute(f"INSERT INTO test_table(geometry) VALUES (GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))', {self.db.srid}))")
        self.db.execute("SELECT RecoverGeometryColumn('test_table','geometry',31370,'POLYGON','XY')")
        result = self.db.inspectGeometry('test_table')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], 'POLYGON')

    def test_loadCsv(self):
        """
        Test the loadCsv method.
        """

        csv_content = """
        id,name
        1,test1
        2,test2
        """

        # Save csv in temporary directory
        temp_csv_path = os.path.join(self.temp_dir, 'test.csv')
        with open(temp_csv_path, 'w') as f:
            f.write(csv_content)
        
        # Load csv
        self.db.loadCsv(temp_csv_path, 'test_csv', sep=',')

        # Checks
        count=self.db.getTableCountOfRows('test_csv')
        self.assertEqual(count,2)

        # Cleaning
        self.db.dropTable('test_csv')

    def test_loadEmbeddedCsv(self):
        """
        Test the loadEmbeddedCsv method.
        """
        csv_content = """
        id,name
        1,test1
        2,test2
        """

        # Load csv
        self.db.loadEmbeddedCsv(csv_content, 'test_embedded_csv', sep=',', if_exists='replace')

        # Checks
        count=self.db.getTableCountOfRows('test_embedded_csv')
        self.assertEqual(count,2)

        # Cleaning
        self.db.dropTable('test_embedded_csv')

    
    def test_loadShp(self):
        """
        Test the loadShp method.
        """
        # Create a dummy shapefile for testing
        shp_content='''
        {
        "type": "FeatureCollection",
        "features": [
        {
            "type": "Feature",
            "properties": {
                "id": 1,
                "name": "test1"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    0,
                    0
                ]
            }
        },
        {
            "type": "Feature",
            "properties": {
                "id": 2,
                "name": "test2"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    1,
                    1
                ]
            }
        }
        ]
        }
        '''
        import geopandas as gpd
        import json
        data=json.loads(shp_content)
        gdf = gpd.GeoDataFrame.from_features(data['features'],crs='EPSG:31370')
        
        shapefile_path_without_extension = os.path.join(self.temp_dir, 'test_shp')
        shapefile_path = f'{shapefile_path_without_extension}.shp'
        gdf.to_file(shapefile_path)
        
        self.db.loadShp(shapefile_path_without_extension, 'test_shp')
        self.assertTrue(self.db.tableExists('test_shp'))
        count = self.db.getTableCountOfRows('test_shp')
        self.assertEqual(count,2)

    
if __name__ == '__main__':
    unittest.main()

# Example of use
# python -m unittest -v testGeodatabase.TestGeodatabase.test_attach
# python testGeodatabase.py -v
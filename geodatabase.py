# -*- coding: utf-8 -*-
"""
Created on Wed Dec 23 17:31:33 2020

@author: Youri.Baeyens
"""
import os
import sqlite3
import logging
import pandas as pd
import sys
from typing import Literal
from mygeodb.graph import connectedComponentsLabeler
from io import StringIO

geom_types={1:'POINT',
            2:'LINESTRING',
            3:'POLYGON',
            4:'MULTIPOINT',
            5:'MULTILINESTRING',
            6:'MULTIPOLYGON',
            7:'GEOMETRYCOLLECTION',
            1001:'POINT Z',
            1002:'LINESTRING Z',
            1003:'POLYGON Z',
            1004:'MULTIPOINT Z',
            1005:'MULTILINESTRING Z',
            1006:'MULTIPOLYGON Z',
            1007:'GEOMETRYCOLLECTION Z',
            2001:'POINT M',
            2002:'LINESTRING M',
            2003:'POLYGON M',
            2004:'MULTIPOINT M',
            2005:'MULTILINESTRING M',
            2006:'MULTIPOLYGON M',
            2007:'GEOMETRYCOLLECTION M',
            3001:'POINT ZM',
            3002:'LINESTRING ZM',
            3003:'POLYGON ZM',
            3004:'MULTIPOINT ZM',
            3005:'MULTILINESTRING ZM',
            3006:'MULTIPOLYGON ZM',
            3007:'GEOMETRYCOLLECTION ZM',
            1000002:'COMPRESSED LINESTRING',
            1000003:'COMPRESSED POLYGON',
            1001002:'COMPRESSED LINESTRING Z',
            1001003:'COMPRESSED POLYGON Z',
            1002002:'COMPRESSED LINESTRING M',
            1002003:'COMPRESSED POLYGON M',
            1003002:'COMPRESSED LINESTRING ZM',
            1003003:'COMPRESSED POLYGON ZM'}

os.environ['SPATIALITE_SECURITY']='relaxed'

class Geodatabase(object):
    """
    Geodatabase.
    
    Available methods:
    
        
    """
    version='22.01'
    
    

    def __init__(self,
                 db: str = ':memory:', 
                 srid: int = 31370, 
                 recreate: bool = False):
        """
        :parameter db:       Path to database. The default is ':memory:'.
        :parameter srid:     The Spatial Reference IDentifier or SRID. 
        :parameter recreate: Should the database be re-created if it exists? 
        
        .. _srid : https://en.wikipedia.org/wiki/Spatial_reference_system
        
        :example:
            
            >>> from geodatabase import geodatabase as gdb
            >>> db=gdb()

        """

        dbExists=os.path.exists(f'{db}')       
        if dbExists and recreate==True:
            os.remove(f'{db}')

        database = sqlite3.connect(f'{db}')
        database.enable_load_extension(True)
        database.load_extension("mod_spatialite")
        if not dbExists or recreate==True:
            database.execute('select InitSpatialMetaData(1);')
            database.execute('select UpdateLayerStatistics();')
        database.commit()

        database.execute('drop table if exists data_licenses')
        #database.execute('VACUUM')

        self.database = database
        self.srid = srid
        
    def attach(self, 
               path: str, 
               alias: str):
        """
        Attach external db.
        
        :example:
            >>> attach('c:\path\to\mydb.sqlite','mydb')

        """
        self.database.execute(f"attach '{path}' as {alias}")
        
        logging.info(f'{alias}:attach:path={path}')

    def checksForTable(self,
                       table: str) -> bool:
        """
        Detects table `without`_ rowid or with `shadowed`_ rowid.
        
        .. _shadowed: https://www.gaia-gis.it/fossil/libspatialite/wiki?name=Shadowed+ROWID+issues
        .. _without:  https://www.gaia-gis.it/fossil/libspatialite/wiki?name=4.2.0+functions#5

        :raises Exception: Exception raised if either shadowed rowid or absence 
                           of rowid raises are detected.
        
        """
        ShadowedRowid, = self.database.execute(f'''
                    select CheckShadowedRowid('{table}')
                    ''').fetchall()[0]

        if ShadowedRowid:
            logging.error(f'{table}:CheckShadowedRowid')
            raise Exception(f'''
                            Table {table} has a physical
                            column named "rowid" (caseless)
                            shadowing the real ROWID.''')
        else:
            logging.info(f'{table}:CheckShadowedRowid')

        WithoutRowid, = self.database.execute(f'''
                    select CheckWithoutRowid('{table}')
                    ''').fetchall()[0]

        if WithoutRowid:
            logging.error(f'{table}:CheckWithoutRowid')
            raise Exception(f'''
                            Table {table} was created by
                            specifying a WITHOUT ROWID clause.''')
        else:
            logging.info(f'{table}:CheckWithoutRowid')
        return(True)

    def close(self):
        """
        Closes the geodatabase.

        """
        
        self.database.execute('select UpdateLayerStatistics();')
        self.database.close()

    def correctGeometry(self,
                        table: str,
                        geometry: str = 'geometry'):
        """
        Correct geometries
        

        """
        
        before=self.geometryCountOfErrors(table,geometry=geometry)
        if before>0:
            logging.info(f'{table}.{geometry}:countOfErrors:{before}')
            logging.info(f'{table}.{geometry}:countOfErrors:trying to correct geometries')
            self.database.execute(f'''
                update {table} SET {geometry} = st_makeValid({geometry})
                where ST_IsValid({geometry}) <>1
                ''')
        after=self.geometryCountOfErrors(table,geometry=geometry)
        logging.info(f'{table}.{geometry}:countOfErrors:{after}')
        if after>0:
            logging.warning(f'{table}.{geometry}:countOfErrors still >0 after correction')
        else:
            logging.info(f'{table}.{geometry}:countOfErrors=0 after correction!')
        
    def createSpatialIndex(self,
                           table: str,
                           geometry: str = 'geometry'):
        """
        Create a spatial index.

        :raises exception: Exception is thrown if the creation of 
                           spatial index failed.

        """
        
        logging.info(f'{table}.{geometry}:createSpatialIndex:start')
        
        SpatialIndex, = self.database.execute(f'''
                    select createSpatialIndex(
                                                '{table}'               ,
                                                '{geometry}'
                                             )
                             ''').fetchone()
        if SpatialIndex:
            logging.info(f'{table}.{geometry}:createSpatialIndex:end')
        else:
            logging.error(f'{table}.{geometry}:createSpatialIndex error.')
            raise Exception(f'Creation of spatial index for {table}.{geometry} failed')
            
    def dropTable(self,
                  table: str):
        
        self.execute(f"select dropTable(NULL,'{table}')")
            
    def execute(self,
                query:str,
                **queryParameters) -> list:
        
        logging.info('query:start')
        
        r=self.database.execute(query.format(**queryParameters))
        
        logging.info('query:end')
        
        return r

    def executeScript(self,
                      query: str,
                      **queryParameters) -> list:
        """
        Execute a sql script. The script can contain many SQLs separated
        by ;.

        :parameter query: Path to the sql query. Do not specify the .sql
                          extension.
            
        :queryParameters: list of "parameter=value" parameters that can be 
                          used inside the query.

        """
        
        logging.info(f'{query}:query:start')
        
        with open(f'../sql/{query}.sql') as f:
            q = f.read().format(**queryParameters)

        r=self.database.executescript(q)
        
        logging.info(f'{query}:query:end')
        
        return r

    def findConnectedComponents(self,
                                inputTable: str,
                                edgesColumnNames :[str,str],
                                outputTable: str
                                ) -> pd.DataFrame:
        """
        Finds connected components of an undirected graph.
        
        :param inputTable: DESCRIPTION
        :type inputTable: str
        :param edgesColumnNames: DESCRIPTION
        :type edgesColumnNames: list
        :param outputTable: DESCRIPTION
        :type outputTable: str
        :return: DESCRIPTION
        
        :rtype: TYPE
        
        :example:
            
            >>> db.findConnectedComponents('t01_edges',['elt_a','elt_b'],'t02_cc')

        """

        logging.info(f'{inputTable}:findConnectedComponents:outputTable={outputTable}:start')

        edges = pd.read_sql(f'''
                                select {edgesColumnNames[0]} as a,
                                       {edgesColumnNames[1]} as b
                                from {inputTable}
                            ''', self.database)
        
        connectedComponents = connectedComponentsLabeler(edges).getConnectedCompontents()
        connectedComponents.to_sql(f'{outputTable}',self.database,index=False,if_exists="replace")

        logging.info(f'{inputTable}:findConnectedComponents:outputTable={outputTable}:end')
        
        return(connectedComponents)


    def geometryCountOfErrors(self,
                              table: str,
                              geometry: str = 'geometry') -> int:

        countOfErrors=self.database.execute(f'''
                              select count(*)
                              from {table}
                              where ST_IsValid({geometry},1)<>1
                              ''').fetchone()[0]
        return(countOfErrors)


    
    def getColumnMaxLength(self,
                           table: str,
                           column: str) -> int:
        return(self.database.execute(f'select max(length({column})) from {table}').fetchone()[0])

    def getColumnCountOfDistinctValues(self,
                           table: str,
                           column: str) -> int:
        return(self.database.execute(f'select count(distinct {column}) from {table}').fetchone()[0])

    def getGeometriesMetadata(self) -> pd.DataFrame:
        """
        Get geometries metadata.

        :returns: DataFrame with available metadata on geometries.
            
        :example:
            
            >>> db.getGeometriesMetadata()
            
            .. code-block::
                
                               f_geometry_column  geometry_type  coord_dimension   srid  spatial_index_enabled
                f_table_name                                                                                  
                t01_zip                 geometry           3003                4  31370                      1
                t04_zip_sample          geometry              3                2  31370                      1

        :example:
            
            >>> db.getGeometriesMetadata().loc['t01_zip',:]
            f_geometry_column        geometry
            geometry_type                3003
            coord_dimension                 4
            srid                        31370
            spatial_index_enabled           1
            Name: t01_zip, dtype: object

        """
        return pd.read_sql('''
                            select f_table_name, 
                                   f_geometry_column, 
                                   geometry_type, 
                                   coord_dimension, 
                                   srid, 
                                   spatial_index_enabled
                            from geometry_columns
                           ''',self.database,index_col='f_table_name')
    
    def getListOfColumns(self,
                         table: str,
                         withLengths: bool = False,
                         withDistincts: bool = False)->pd.DataFrame:
        """
        
        Documents the structure and content of a table. The returned 
        pandas DataFrame has got this structure:
            * cd_column_type: ['INTEGER','DOUBLE','TEXT','POLYGON',...]
            * fl_notnull_constraint: 1=nulls are not allowed, 0=nulls allowed
            * default_value
            * fl_primary_key: 1=a primary key, 0=not a primary key
            * ms_max_length: max length of column expressed in characters or bytes
        
        :example:
            
            >>> db.getListOfColumns('t01_zip',withLengths=True)
            
        .. code-block:: 
            
                           cd_column_type   fl_primary_key  ms_max_length
            tx_column_name                                                                                   
            pk_uid                INTEGER                1            4.0
            join_count            INTEGER                0            5.0
            nouveau_po               TEXT                0            4.0
            frequency             INTEGER                0            2.0
            cp_speciau            INTEGER                0            1.0
            shape_leng             DOUBLE                0           13.0
            shape_area             DOUBLE                0           13.0
            geometry              POLYGON                0       142708.0
        
        """
        
        if table.upper() not in [t.upper() for t in self.getListOfTables().index]:
            print(f'{table} is not a table')
            return()
        
        listOfColumns=pd.read_sql(f'''
                              select name       as tx_column_name,
                                     type       as cd_column_type,
                                     "notnull"  as fl_notnull_constraint,
                                     dflt_value as default_value,
                                     pk         as fl_primary_key
                              from pragma_table_info('{table}')
                             ''',self.database,index_col='tx_column_name')

        listOfColumns = listOfColumns.astype({'fl_notnull_constraint':int,
                                              'fl_primary_key':int})

        if withLengths:
            listOfColumnsElt=listOfColumns.loc()
            for row in listOfColumns.itertuples():
                listOfColumnsElt[row.Index,'ms_max_length']=self.getColumnMaxLength(table,row.Index)

            listOfColumns = listOfColumns.astype({'ms_max_length': int})

        if withDistincts:
            listOfColumnsElt=listOfColumns.loc()
            for row in listOfColumns.itertuples():
                listOfColumnsElt[row.Index,'ms_countOf_distinct']=self.getColumnCountOfDistinctValues(table,row.Index)

            listOfColumns = listOfColumns.astype({'ms_countOf_distinct': int})

        
        return(listOfColumns)

    def getListOfTables(self) -> pd.DataFrame:
        """
        Get list of tables in geodatabase (including indexes and count of rows).

        :example:
            
            >>> db.getListOfTables()
            
        .. code-block::
            
                           ms_countOf_geometries   ms_countOf_spatial_indexes   ms_countOf_rows
            f_table_name
            T01_zip                             1                           1           1268.0
            T02_PIP                             0                           0         225263.0
            t03_sample                          0                           0         225262.0
            t04_sample                          0                           0              2.0
            t04_zip                             1                           1             10.0

        """
        listOfTables=pd.read_sql("""
            select a.type,
                   a.f_table_name,
                   count(distinct f_geometry_column) as ms_countOf_geometries,
                   coalesce(sum(spatial_index_enabled),0) as ms_countOf_spatial_indexes
            from (
                  select type, name as f_table_name
                  from sqlite_master 
                  where type in ('table','view') 
                  and name NOT LIKE 'sqlite_%'
                  and name NOT LIKE 'idx_%'
                  and name NOT IN (
                    'ElementaryGeometries',
                    'geometry_columns',
                    'geometry_columns_auth',
                    'geometry_columns_field_infos',
                    'geometry_columns_statistics',
                    'geometry_columns_time',
                    'KNN',
                    'spatial_ref_sys',
                    'spatial_ref_sys_aux',
                    'SpatialIndex',
                    'spatialite_history',
                    'sql_statements_log',
                    'views_geometry_columns',
                    'views_geometry_columns_auth',
                    'views_geometry_columns_field_infos',
                    'views_geometry_columns_statistics',
                    'virts_geometry_columns',
                    'virts_geometry_columns_auth',
                    'virts_geometry_columns_field_infos',
                    'virts_geometry_columns_statistics',
                    'geom_cols_ref_sys',
                    'spatial_ref_sys_all',
                    'vector_layers',
                    'vector_layers_auth',
                    'vector_layers_field_infos',
                    'vector_layers_statistics'
                                    )
                  ) a LEFT JOIN geometry_columns b
            on lower(a.f_table_name)=lower(b.f_table_name)
            group by 1, 2
                                     """,self.database,index_col='f_table_name')
                                     
        listOfTablesElt=listOfTables.loc()
        
        for row in listOfTables.itertuples():
            listOfTablesElt[row.Index,'ms_countOf_rows']=self.getTableCountOfRows(row.Index)
    
        if listOfTables.shape[0]>0:
            listOfTables = listOfTables.astype({'ms_countOf_geometries': int,
                                                'ms_countOf_spatial_indexes':int,
                                                'ms_countOf_rows':int})
        
        return(listOfTables)

    def getTableCountOfRows(self,
                            table: str) -> int:
        """
        Counts the number of rows in table.
            
        :example:
            
            >>> db.getTableCountOfRows('T01_ZIP')
            1268

        """
        return self.database.execute(f'select count(*) from {table}').fetchone()[0]

    def getTableHead(self,
                     table: str) -> pd.DataFrame:

        return pd.read_sql(f'select rowid, * from {table} limit 10',self.database,index_col="rowid")
    
            
    def inspectGeometry(self,
                        table: str,
                        geometry: str = 'geometry') -> list:
        """
        Inspects the nature of a table geometry.

        :return: List of *(geomType, srid, coordDimension, ms_countof_rows)* 
                 t-uples.
        
        :example:
            
            >>> db.inspectGeometry('T01_ZIP')
            [('POLYGON ZM', 31370, 'XYZM', 1268)]
        
        .. warning:: 
            
            a Geometry can contain various types of geometries 
            (eg: both Polygons and Multipolygons). As a consequence
            the returned list can contain multiple elements.

        """
        res=self.database.execute(
            f'''
                select GeometryType("{geometry}")   as geomType, 
                       Srid("{geometry}")           as srid, 
                       CoordDimension("{geometry}") as coordDimension,
                       count(*)                     as ms_countof_rows
                from {table}
                where {geometry} is not null
                group by 1, 2, 3
             ''').fetchall()
        
        return(res)
    
    def loadCsv(self,
                csvFile: str,
                table: str,
                if_exists: Literal['fail','replace','append'] = 'replace',
                **readParameters):
        """
        Loads csv file and into a table.

        :parameter csvFile:            Path to csv file. File extension should 
                                       be mentionned.
        :parameter table:              Table name.
        :parameter if_exists:          What to do if table already exists. 
        :parameter readParameters:     Parameters for the pandas.read_csv 
                                       function.
        
        :return: void
        
        :example:
            
            >>> dtype={
            >>>        'ADR_ID':'str', 
            >>>        'ADR_CREAT':'str', 
            >>>        'ADR_MODIF':'str',
            >>>        'X':np.float64, 
            >>>        'Y':np.float64
            >>>       }
            >>> parse_dates=['ADR_CREAT', 'ADR_MODIF']
            >>> db.loadCsv('myFile.csv',
            >>>            'myTable',
            >>>            dtype=dtype,
            >>>            parse_dates=parse_dates,
            >>>            sep='|')

        """
        
        logging.info(f'{table}:loadCsv:csvFile={csvFile}:start')
        
        pd.read_csv(csvFile,
                    **readParameters
                    ).to_sql(table,self.database,if_exists=if_exists,index=False)
        
        logging.info(f'{table}:loadCsv:csvFile={csvFile}:end')

    def loadDataFrame(self,
                      DataFrame: pd.DataFrame,
                      table: str,
                      if_exists: Literal['fail','replace','append'] = 'replace',
                      **readParameters):
        pass

    def loadEmbeddedCsv(self,
                        file: str,
                        table: str,
                        if_exists: Literal['fail','replace','append'] = 'replace',
                        **readParameters):
        
        """
        Loads an embedded csv file and into a table.

        :parameter file:               Embedded csv file.
        :parameter table:              Table name.
        :parameter if_exists:          What to do if table already exists. 
        :parameter readParameters:     Parameters for the pandas.read_csv 
                                       function.
        
        :return: void
        
        :example:
            
            >>> file='''
            >>> cd_sex|tx_sex
            >>> M|Male
            >>> F|Female
            >>> '''
            >>> db.loadEmbeddedCsv(file,'t_sex',sep='|')

        """

        logging.info(f'{table}:loadEmbeddedCsv:start')
        
        csvFile=StringIO(file)
        pd.read_csv(csvFile, 
                    **readParameters
                    ).to_sql(table,self.database,if_exists=if_exists,index=False)

        logging.info(f'{table}:loadEmbeddedCsv:end')

    def loadShp(self,
                shapeFile: str,
                table: str,
                encoding: str = 'CP1252',
                srid: int = None):
        """
        Loads a shapefile and spatially indexes the resulting table.
        
        :parameter shapeFile: Path to shapefile. File extension should be 
                              .shp and cannot be mentionned in shapeFile.
        :parameter table:     Name of the table to be created. This table 
                              will be automatically spatially indexed.
        :parameter encoding:  The character encoding adopted by the DBF 
                              member, as e.g. UTF-8 or CP1252. 
        :parameter srid:      EPSG SRID value. 

        :return: void
        
        
        :example:
        
            >>> import geodatabase as gdb 
            >>> db=gdb.geodatabase()
            >>> db.loadShp('C:/.../postaldistricts','T01_zip') 
            Loading shapefile at 'C:/.../postaldistricts' into SQLite table 'T01_zip'
            Inserted 1268 rows into 'T01_zip' from SHAPEFILE
        
        """
    
        logging.info(f'{table}:loadShp:shapeFile={shapeFile}:start')
    
        if not srid:
            srid = self.srid
            
        self.database.execute(f"""
            select ImportSHP('{shapeFile}','{table}','{encoding}',{srid})
            """).fetchall()
        
        logging.info(f'{table}:loadShp:shapeFile={shapeFile}:end')
        
        #self.createSpatialIndex(table)
            
    def loadShpZip(self):
        pass
    

    def pointInPolygon(self,
                       points: str,
                       polygons: str,
                       outputTable: str):

        """
        Assigns point to polygons.

        :parameter points: Name of the table containing points.
        :parameter polygons: Name of the table containing polygons.
        :parameter outputTable: Name of the output table.
        :parameter queryParameters: List of "parameter=value". Those 
                  parameters can be used inside the query.

        :returns: void.
        
        .. warning::
            
            Do not VACUUM your database ! If you do, you risk to break
            your link table.

        """
        
        logging.info(f'{outputTable}:pointInPolygon:points={points}:polygons={polygons}:start')
        
        self.database.execute(f"""
            create table {outputTable} as
            select pt.rowid as rowid_point, pg.rowid as rowid_polygon
            from {points} as pt, {polygons} as pg
            where st_within(pt.geometry,pg.geometry)
            and pt.rowid in ( select rowid
                              from spatialIndex
                              where f_table_name='DB={points}'
                                and search_frame=pg.geometry )
        """)
        
        logging.info(f'{outputTable}:pointInPolygon:points={points}:polygons={polygons}:end')


    def recoverGeometry(self,
                        table: str,
                        geometry: str = 'geometry'):
        """
        Recovers geometry, ie, registers the geometry in spatialite's metadata.
        

        :param table:    Name of the table containing the geometry.
        :param geometry: Name of the geometry column.

        :raises exception: Exception is raised if geometry cannot be recovered.

        :returns: void

        """
        
        inspection=self.inspectGeometry(table,geometry)
        
        if len(inspection)>1:
            logging.error(f'{table}.{geometry}:recoverGeometryColumn failed:More than 1 geometry type')
            raise Exception(f'Recovery of {table}.{geometry} failed:More than 1 geometry type')
             
        geomType,srid,coordDimension,ms_countof_rows=inspection[0]
        
        if geomType not in geom_types.values():
            logging.error(f'{table}.{geometry}:recoverGeometryColumn failed:Not a geometry')
            raise Exception(f'Recovery of {table}.{geometry} failed:Not a geometry')

        if srid<=0:
            logging.warning(f'{table}.{geometry}:recoverGeometryColumn failed:Srid unknown-Choosing {self.srid}')
            srid = self.srid
            
        geomType=geomType.replace(' ','')
        coordDimension=coordDimension.replace(' ','')

        RecoverGeometry, = self.database.execute(f'''
            select recoverGeometryColumn(
                                         '{table}'         ,
                                         '{geometry}'      ,
                                          {srid}           ,
                                         '{geomType}'      ,
                                         '{coordDimension}'
                                         )
                     ''').fetchone()

        if RecoverGeometry:
            logging.info(f'{table}.{geometry}:recoverGeometryColumn:geometry has been recovered')
        else:
            logging.error(f'{table}.{geometry}:recoverGeometryColumn')
            raise Exception(f'Recovery of {table}.{geometry} failed')
            


    def recoverSpatialIndex(self,
                            table: str,
                            geometry: str = 'geometry'):
        """
        Recover Spatial Index.
                
        A delete operation followed by a VACCUM operation could leave an index 
        in an inconsistent state.
        
        This method checks for the consistency of the spatial index. If the 
        spatial index is inconsistent then try to recover the index (ie, rebuild). 
        If index recovery failed then an exception is thrown.

        :raises Exception: Raises an Exception if geometry is inconsistent.

        .. warning::
            Inconsistent indexes can have a dramatic impact. Read
            `this note on SpatialIndexes <http://www.gaia-gis.it/gaia-sins/SpatialIndex-Update.pdf>`_ 
            carefully! Please, run this method if you have
            a doubt on the integrity of your spatial index.
            
        """
        RecoveredSpatialIndex, = self.database.execute(f'''
            select recoverSpatialIndex('{table}','{geometry}')
            ''').fetchall()[0]

        if RecoveredSpatialIndex:
            logging.info(f'{table}.{geometry}:recoverSpatialIndex:Spatial index is consistent')
        else:
            logging.error(f'{table}.{geometry}:recoverSpatialIndex')
            raise Exception(f'Spatial index {table}.{geometry} is inconsistent')
            
    def reproject(self,
                  inputTable: str,
                  srid: int,
                  outputTable: str = None,
                  if_exists: Literal['fail','replace'] = 'fail'
                  ):
              
        """
        Reproject.
        
        This method reprojects a geometry.
        
        :param inputTable:    Name of the table containing the geometry to reproject.
        :param srid:          Reproject into the Reference System identified by srid.
        :param outputTable:   Name of the output table containing the reprojected geometry.

        :returns: void
        """
    
        if not outputTable:
                outputTable = f'{inputTable}_{srid}'
                
        logging.info(f'{inputTable}:reproject:srid={srid}:outputTable={outputTable}:start')
        
        if self.tableExists(outputTable):
            if if_exists=='fail':
                logging.error(f'{inputTable}:reproject:{outputTable} already exists !')
                raise Exception(f'''{outputTable} already exists !''')
            if if_exists=='replace':
                self.dropTable(outputTable)
                logging.warning(f'{inputTable}:reproject:{outputTable} dropped before reproject takes place')

        l=list(self.getListOfColumns(inputTable).index)
        l.remove('geometry')
        
        self.database.execute(f'''
                                create table {outputTable} as
                                select {','.join(l)},st_transform(geometry,{srid}) as geometry
                                from {inputTable}
                               ''')

        logging.info(f'{inputTable}:reproject:srid={srid}:outputTable={outputTable}:end')
                    
        self.recoverGeometry(outputTable)
        self.createSpatialIndex(outputTable)
        
    def tableExists(self,
                    table: str):
                    
        if table in self.getListOfTables().index:
            return True
        else:
            return False

    def toCsv(self,
              table: str,
              archive_name: str = None,
              mode: Literal['w','a']  = 'w',
              outputDirectory: str = '.',
              columnsToExclude: list[str] = ['geometry']):
        """
        Exports a table to a csv file.

        :parameter table:            Table name.
        :parameter outputDirectory:  Output directory. 
        :parameter columnsToExclude: Columns to exclude from export. 

        :returns: void
        
        :example:
            
            >>> db.toCsv('T01_table','../output')

        """

        logging.info(f'{table}:toCsv:start')
        
        if not archive_name:
            archive_name=table
        
        df=pd.read_sql(f'select * from {table}',self.database)  
        
        #columnsToExport=set(df.columns).difference(columnsToExclude) 
        columnsToExport=[col for col in df.columns if col not in columnsToExclude]
        
        compression_options = dict(method='zip', archive_name=f'{table}.csv')
        df.loc[:,columnsToExport].to_csv(f'{outputDirectory}/{archive_name}.zip',
                                         sep='|',
                                         encoding='utf-8',
                                         index=False,
                                         compression=compression_options,
                                         mode=mode)
        
        logging.info(f'{table}:toCsv:end')
             
        
    def toSas(self,
              table: str,
              archive_name: str = None,
              outputDirectory: str = '.',
              columnsToExclude: list[str] = ['geometry']):
        """
        Create as sas program to import the csv export of a table.

        :parameter table: Table name.
        :parameter outputDirectory: Output directory. The default is '.'.
        :parameter columnsToExclude: Columns to exclude from sas import. The
                                     geometry column is excluded by default
                                     as geometries cannot be imported in sas.
        
        :return: void
        
        :example:
            
            >>> db.toSas('t03_pip_sample')
        
        Content of ./t01_zip.sas
        
        .. code-block:: sas
        
            filename myZip ZIP 't03_pip_sample.zip';
            
            data t03_pip_sample(compress=YES);
            length 
                    id_rec                                      8         
                    rowid_point                                 8         
                    rowid_polygon                               8         
                    ;
            infile myZip(t03_pip_sample.csv) 
                   dsd 
                   delimiter='|' 
                   encoding='utf8' 
                   firstobs=2 
                   missover 
                   lrecl=10000;
            input
                    id_rec                                  
                    rowid_point                             
                    rowid_polygon                           
                    ;
            run;

        """

        logging.info(f'{table}:toSas:start')

        if not archive_name:
            archive_name=table
                
    
        structure={} # Un dictionnaire dont les keys sont les noms de colonnes et les valeurs, les longueurs maximales
        
        for row in self.getListOfColumns(table,withLengths=True).itertuples():
            if row.cd_column_type in ('TEXT','BLOB',''):
                structure[row.Index]='$'+str(row.ms_max_length)
            else:
                if row.cd_column_type in ('INTEGER','DOUBLE','REAL','INT','NUM'):
                    structure[row.Index]='8'
                else:
                    structure[row.Index]='Unknown'
        
        for d in [c for c in structure.keys() if c in columnsToExclude]:
            del(structure[d])
        
        with open(f'{outputDirectory}/{table}.sas', 'w') as f:
            f.write(f"filename myZip ZIP '{archive_name}.zip';\n")
            f.write(f"data {table}(compress=YES);\n")
            f.write("length \n")
            for variable in structure.keys():
                f.write(f'        {variable:40}    {structure[variable]:10}\n')
            f.write("        ;\n")
            f.write(f"infile myZip({table}.csv) dsd delimiter='|' encoding='utf8' firstobs=2 missover lrecl=10000;\n")
            f.write("input\n") 
            for variable in structure.keys():
                f.write(f'        {variable:40}\n')
            f.write("        ;\n")
            f.write("run;\n")

        logging.info(f'{table}:toSas:end')
    
    def toSpatialite(self,
                     db,
                     table: str):
        pass    
        

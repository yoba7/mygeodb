Welcome to mygeodb
==================

In the same way that ``postgis`` adds spatial functionality to ``postgresql``, ``libspatialite`` is a library which adds spatial functionality to ``sqlite``. A ``spatialite`` database is an ``sqlite`` database created with the ``libspatialite`` library and containing spatial data (i.e. polygons, points, etc.).

``spatialite`` is a development by Alessandro Furieri. Here is the link to the [web site of Alessandro](https://www.gaia-gis.it/fossil/libspatialite/index). Many thanks to him for this incredible tool !

The purpose of this package is to facilitate interaction with the ``libspatialite`` library. For example, it allows you to :
 - simplify the recording of geometries in the metadata layer
 - simplify the creation of spatial indexes
 - simplify the loading of shapefiles
 - simplify the export of tables in csv format
 - execute sql queries stored in external scripts
 - obtain information about the content of a spatial database (list of tables, geometry inventory, etc.)
 - ...

Installation of libspatialite
-----------------------------

In order to use ``mygeodb``, you'll need to install the ``libspatialite`` in your Python distribution. I recommand those two methods to install libspatialite:
 - directly via the conda package manager (version 5+ of the library)
 - indirectly via the installation QGIS (or QGIS package, that can easy be installed with the conda package manager).
 
Installation of mygeodb
-----------------------

Just download and add mygeodb to your [sys.path](https://docs.python.org/3/library/sys_path_init.html).

 
Documentation
-------------

The documentation is available here: https://yoba7.github.io/mygeodb-doc/

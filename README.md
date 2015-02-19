# plpyDataManager
A set of functions to easily store and retrieve json objects in postgreSQL plpython functions

## Functionality
plpyDataManager aims to make it easy to store and retreive json objects when working in plpython in postgreSQL

Terms/concepts are in line with MongoDB - json 'documents' are stored in 'collections' using an 'id' for storage/retrieval

Python functions are created and stored in GD so that they may be invoked easily without calling plpy.execute():
* GD\['save'\]\(collection,id,document\) - save json
* document = GD\['load'\]\(collection,id\) - retrieve json
* GD\['drop'\]\(collection\) - remove a collection
* iscached = GD\['iscached'\]\(collection,id\) - check if json is available in a cache or stored only in a table

Data is stored in a database table and cached in GD for up to 5 days:
* a table named datamgr_{collection} is created on the fly when a collection is first used
* a full history is maintained in the table to track changes over time
* the table maintains a partial index on current records for each id
* the first request each day creates a new cache in GD named datamgr_{collection}{YYmmdd}
* load requests are served from the current day's cache if possible, otherwise other recent caches, otherwise the database table
* all load or save requests result in the data being moved to the current day's cache
* when new caches are created each day any caches older than 5 days are removed 

## Code Files
* plpython2u - start_datamanager.sql (CREATE FUNCTION to be run against the database)
* plpython2u - example_use_of_datamanager.sql (Example use of the functions in a plpython function)


-- DROP FUNCTION example_use_of_datamanager()
CREATE OR REPLACE FUNCTION example_use_of_datamanager()
  RETURNS character varying AS
$BODY$

import json

# always include this line at the start of any plpy function using datamanager
if 'datamgr_running' not in GD: plpy.execute("SELECT start_datamanager('');")

# create some data to be stored
myuserdata = {'id':123,'firstName':'Joe','lastName':'Bloggs','friends':['Sam','John','Mike']}

#########################################################
# save the data in a collection
# there is no need to create the collection in advance - 
# it will be created on the fly as required
# syntax: GD["save"](collection,id,doc)

GD["save"]('users','123',myuserdata)

# the data will be cached in GD for easy retrieval
# any data not accessed for 5 days will be removed from the cache automatically
# all data will also be stored in a table named datamgr_{collection} 
# (e.g. table name = 'datamgr_users' in the example above)
# the table will store a history of changes over time and is indexed on id (for current values)

#########################################################
# load data from a collection
# syntax: GD["load"](collection,id)

myuserdata_retrieved = GD["load"]('users','123')

# the data will be retrieved from GD if found in the cache
# otherwise it will be loaded from the table, cached and returned
# if no data is found for the id in the colletion then None is returned

#########################################################
# to facilitate clearing down the cache, there is a daily cache for each collection
# the first request each day creates a new cache for the collection and removes caches older than 5 days
# to check whether data is cached (and to confirm the age of the cache):
# syntax: GD["iscached"](collection,id)

iscached = GD["iscached"]('users','123')

# if found, the function will return the name of the cache: 'datamgr_{collection}{YYYYmmdd}'
# e.g. 'datamgr_users20150219' 
# otherwise the function will return an empty string

#########################################################
# to drop a collection (remove all cached data and drop the table):
# syntax: GD["drop"](collection)

GD["drop"]('users')

#########################################################
# to add a prefix to the GD function names, include the prefix 
# when invoking start_datamanager.  For example, by changing the initial command to:

if 'datamgr_running' not in GD: plpy.execute("SELECT start_datamanager('myprefix_');")

# the functions will become:
# GD["myprefix_save"](collection,id,doc)
# GD["myprefix_load"](collection,id)
# GD["myprefix_iscached"](collection,id)
# GD["myprefix_drop"](collection)

$BODY$
  LANGUAGE plpython2u VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION example_use_of_datamanager()
  OWNER TO postgres;
  


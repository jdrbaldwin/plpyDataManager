
CREATE OR REPLACE FUNCTION start_datamanager(prefix TEXT)
  RETURNS boolean AS
$BODY$

# define the load function
def fload(collection,id):
    try:
        # return the requested data
        return GD["loadfrom"+collection](id)
    except:
        # the loadfromcollection function has not been created so we 
        # need to ensure that all dependencies are in place and then create it

        # check that the table to save the collection exists
        tablename = "datamgr_" + collection
        tableexists = plpy.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '%s');" % (tablename))[0]["exists"]
        if tableexists is False:
            # need to create the table to manage the collection        
            plpy.execute("CREATE SEQUENCE seq_pk_datamgr_%s INCREMENT 1 START 1;" % (collection))
            plpy.execute("CREATE TABLE datamgr_%s (uid INTEGER PRIMARY KEY DEFAULT nextval('seq_pk_datamgr_%s'), id TEXT  NOT NULL, doc JSON  NOT NULL, is_current BOOLEAN  NOT NULL, created_date TIMESTAMP NOT NULL);" % (collection,collection))
            plpy.execute("CREATE UNIQUE INDEX un_datamgr_%s_id ON datamgr_%s (id) WHERE is_current;" % (collection,collection))

        # create the loadfromcollection function
        def floadfromcollection(id):
            from datetime import datetime
            import json
            currentcache = 'datamgr_' + collection + datetime.now().strftime('%Y%m%d')
            # return the requested data from the current_date cache if available
            try:
                return GD[currentcache][id]                
            except:
                # if failed because currentcache does not exist then create it before continuing
                if currentcache not in GD:
                    # create the cache
                    GD[currentcache] = {}
                    # maintain information about the cache
                    if 'datamgr_cachelist' not in GD: GD['datamgr_cachelist'] = {}
                    if collection not in GD['datamgr_cachelist']: GD['datamgr_cachelist'][collection] = []
                    GD['datamgr_cachelist'][collection].append(currentcache)
                    # remove any caches that are not one of the most-recent 5
                    for cachename in sorted([cachename for cachename in GD['datamgr_cachelist'][collection]],reverse=True)[5:]:
                        del GD[cachename]
                        GD['datamgr_cachelist'][collection].remove(cachename)

            # now look in all older cache for this collection, working backwards in time
            for cachename in sorted([cachename for cachename in GD['datamgr_cachelist'][collection]],reverse=True)[1:]:
                if id in GD[cachename]:
                    # move the data to the current cache and return it
                    returndata = GD[cachename][id]
                    GD[currentcache][id] = returndata
                    del GD[cachename][id]
                    return returndata                    

            # if execution reaches this points then data is not cached so look in the table
            try:
                returndata = json.loads(plpy.execute("SELECT doc FROM datamgr_%s WHERE id = '%s' and is_current = True;" % (collection,id))[0]["doc"])
            except:
                returndata = None

            # cache the result and return it
            GD[currentcache][id] = returndata
            return returndata                    
                                                            
        # store the function in GD
        key = 'loadfrom' + collection
        GD[key] = floadfromcollection

    # now invoke the function to return the requested data
    return GD[key](id)
    
# store the function in GD
key = prefix + 'load'
GD[key] = fload

# define the save function
def fsave(collection,id,doc):
    try:
        # save the requested data
        return GD["saveinto"+collection](id,doc)
    except:
        # the saveintocollection function has not been created so we 
        # need to ensure that all dependencies are in place and then create it

        # check that the table to save the collection exists
        tablename = "datamgr_" + collection
        tableexists = plpy.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '%s');" % (tablename))[0]["exists"]
        if tableexists is False:
            # need to create the table to manage the collection        
            plpy.execute("CREATE SEQUENCE seq_pk_datamgr_%s INCREMENT 1 START 1;" % (collection))
            plpy.execute("CREATE TABLE datamgr_%s (uid INTEGER PRIMARY KEY DEFAULT nextval('seq_pk_datamgr_%s'), id TEXT  NOT NULL, doc JSON  NOT NULL, is_current BOOLEAN  NOT NULL, created_date TIMESTAMP NOT NULL);" % (collection,collection))
            plpy.execute("CREATE UNIQUE INDEX un_datamgr_%s_id ON datamgr_%s (id) WHERE is_current;" % (collection,collection))

        # create the saveintocollection function
        def fsaveintocollection(id,doc):
            from datetime import datetime
            import json
            currentcache = 'datamgr_' + collection + datetime.now().strftime('%Y%m%d')
            # save the requested data into the current_date cache if available
            try:
                GD[currentcache][id] = doc
            except:
                # if failed because currentcache does not exist then create it before continuing
                if currentcache not in GD:
                    # create the cache
                    GD[currentcache] = {}
                    # maintain information about the cache
                    if 'datamgr_cachelist' not in GD: GD['datamgr_cachelist'] = {}
                    if collection not in GD['datamgr_cachelist']: GD['datamgr_cachelist'][collection] = []
                    GD['datamgr_cachelist'][collection].append(currentcache)
                    # remove any caches that are not one of the most-recent 5
                    for cachename in sorted([cachename for cachename in GD['datamgr_cachelist'][collection]],reverse=True)[5:]:
                        del GD[cachename]
                        GD['datamgr_cachelist'][collection].remove(cachename)

            # now save the data in the cache
            GD[currentcache][id] = doc

            # and save the data in the table
            ts = str(datetime.now())
            try:
                # close off any current record
                plpy.execute("UPDATE datamgr_%s SET is_current = False WHERE id = '%s' and is_current = True;" % (collection,id))
                plpy.execute("COMMIT;")
                plpy.execute("INSERT INTO datamgr_%s (id,doc,is_current,created_date) VALUES ('%s','%s',True,'%s');" % (collection,id,json.dumps(doc),ts))
            except:
                return False

            return True
                                                            
        # store the function in GD
        key = 'saveinto' + collection
        GD[key] = fsaveintocollection

    # now invoke the function to save the requested data
    return GD[key](id,doc)
    
# store the function in GD
key = prefix + 'save'
GD[key] = fsave

# define the drop function
def fdrop(collection):
    try:
        # need to create the table to manage the collection        
        plpy.execute("DROP INDEX IF EXISTS un_datamgr_%s_id;" % (collection))
        plpy.execute("DROP TABLE IF EXISTS datamgr_%s;" % (collection))
        plpy.execute("DROP SEQUENCE IF EXISTS seq_pk_datamgr_%s;" % (collection))
    except:
        return False

    # drop any cached data
    if 'datamgr_cachelist' not in GD: GD['datamgr_cachelist'] = {}
    if collection in GD['datamgr_cachelist']:
        for cachename in GD['datamgr_cachelist'][collection]:
            del GD['datamgr_cachelist'][collection][cachename]
        del GD['datamgr_cachelist'][collection]
    
# store the function in GD
key = prefix + 'drop'
GD[key] = fdrop

# define the iscached function
def fiscached(collection,id):
    try:
        for cachename in sorted([cachename for cachename in GD['datamgr_cachelist'][collection]],reverse=True):
            if id in GD[cachename]: return cachename
        return ''
    except:
        return ''
    
# store the function in GD
key = prefix + 'iscached'
GD[key] = fiscached

# store an indicator the datamanager is running
GD["datamgr_running"] = True

# clear all caches
for key in [key for key in GD if key[0:8] == 'loadfrom']: del GD[key]
for key in [key for key in GD if key[0:8] == 'saveinto']: del GD[key]
for key in [key for key in GD if key[0:8] == 'datamgr_']: del GD[key]

return True

$BODY$
  LANGUAGE plpython2u VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION start_datamanager(prefix TEXT)
  OWNER TO postgres;
  
SELECT start_datamanager(''); 

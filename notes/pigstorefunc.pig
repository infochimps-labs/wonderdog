--
-- Doesn't work at the moment, just some notes on how the storefunc might look.
--


--
-- Right now the ElasticSearchOutputFormat gets all its options from the
-- Job object. We can use the call to setStoreLocation in the storefunc
-- to set the required parameters. Need to make sure the following are
-- set:
-- 
-- wonderdog.index.name  - should be set by the storefunc constructor
-- wonderdog.bulk.size   - should be set by the storefunc constructor
-- wonderdog.field.names - should be set by the call to checkSchema
-- wonderdog.id.field    - should be set by the storefunc constructor
-- wonderdog.object.type - should be set by the storefunc constructor
-- wonderdog.plugins.dir - should be set by call to setStoreLocation
-- wonderdog.config      - should be set by call to setStoreLocation
--
-- FIXME: options used in the ElasticSearchOutputFormat should NOT be
-- namespaced with 'wonderdog'

%default INDEX 'es_index'
%default OBJ   'text_obj'

        
records         = LOAD '$DATA'   AS (text_field:chararray);
records_with_id = LOAD '$IDDATA' AS (id_field:int, text_field:chararray);

-- Here we would use the elasticsearch index name as the uri, pass in a
-- comma separated list of field names as the first arg, the id field
-- as the second arg and the bulk size as the third. 
-- 
-- and so on.
STORE records INTO '$INDEX/$OBJ' USING ElasticSearchStorage('my_text_field', '-1', '1000');


-- but it would be really nice to duplicate what's in WonderDog.java in that,
-- should a bulk request fail, the failed records are written to hdfs. The
-- user should have some control of this. Also, it should be possible to generate
-- the field names directly from the pig schema? (We'd have to be VERY explicit in the
-- docs about this as it would be a point of headscratching/swearing...) In this
-- case we might have something like:
named_records = FOREACH records GENERATE text_field AS text_field_name;
STORE records INTO '/path/to/failed_requests' USING ElasticSearchStorage('$INDEX/$OBJ', '-1', '1000');

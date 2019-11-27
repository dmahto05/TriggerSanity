WITH ALIAS1 AS 
(select event_object_schema as table_schema,
       event_object_table as table_name,
       trigger_schema,
       trigger_name,
       string_agg(event_manipulation, ',') as event_manipulation,
       action_timing as activation,
       action_condition as condition,
       action_statement as definition 
from information_schema.triggers
where event_object_schema = 'ibis_ul_metadata'
group by 1,2,3,4,6,7,8
order by table_schema,
         table_name)
select ALIAS1.*, 'BEGIN; ' || case 
	   when event_manipulation like '%INSERT%' AND event_manipulation NOT like '%DELETE%'
 AND event_manipulation NOT like '%UPDATE%' THEN 
	   (
		   SELECT  'INSERT INTO ' || table_schema || '.' || table_name || '('  || 
		   	STRING_AGG(column_name , ',' ORDER BY  parameters.ordinal_position)
		   	  || ') VALUES ('  || 
		   STRING_AGG(CASE
		      WHEN parameters.data_type IN ('smallint' , 'integer' , 'bigint' , 'decimal' , 'numeric' , 'real' , 'double precision' , 'smallserial' , 'serial' , 'bigserial')
		      THEN '10' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('money')
		      THEN '99.07' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('character' , 'character varying' , 'varchar' , 'char' , 'text')
		      THEN '''' || 1 || '''' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('bytea')
		      THEN 'E' || ''''|| '\\000' || '''' || '::bytea'
		      WHEN parameters.data_type IN ('date' , 'time with time zone' , 'time without time zone' , 'timestamp with time zone','timestamp without time zone')
		      THEN '''' || 'now' || '''' || '::' || parameters.data_type
		      WHEN parameters.data_type in ('boolean')
		      THEN 'true::boolean'
		      WHEN parameters.data_type in ('UUID')
		      THEN 'uuid_generate_v4 ()'
		      ELSE ' '
		      END , ',' ORDER BY parameters.ordinal_position) || '); ROLLBACK;' 
		     FROM information_schema.columns as parameters
		    WHERE table_schema = ALIAS1.table_schema
		      AND table_name   = ALIAS1.table_name
		      and is_nullable = 'NO'
		      GROUP BY table_schema , table_name LIMIT 1
	   )
	   WHEN event_manipulation like '%UPDATE%' AND event_manipulation NOT like '%DELETE%'
 AND event_manipulation NOT like '%INSERT%' THEN
	   (
		   SELECT  'INSERT INTO ' || table_schema || '.' || table_name || '('  || 
		   	STRING_AGG(column_name , ',' ORDER BY  parameters.ordinal_position)
		   	  || ') VALUES (' ||
		   STRING_AGG(CASE
		      WHEN parameters.data_type IN ('smallint' , 'integer' , 'bigint' , 'decimal' , 'numeric' , 'real' , 'double precision' , 'smallserial' , 'serial' , 'bigserial')
		      THEN '10' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('money')
		      THEN '99.07' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('character' , 'character varying' , 'varchar' , 'char' , 'text')
		      THEN '''' || 1 || '''' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('bytea')
		      THEN 'E' || ''''|| '\\000' || '''' || '::bytea'
		      WHEN parameters.data_type IN ('date' , 'time with time zone' , 'time without time zone' , 'timestamp with time zone','timestamp without time zone')
		      THEN '''' || 'now' || '''' || '::' || parameters.data_type
		      WHEN parameters.data_type in ('boolean')
		      THEN 'true::boolean'
		      WHEN parameters.data_type in ('UUID')
		      THEN 'uuid_generate_v4 ()'
		      ELSE ' '
		      END , ',' ORDER BY parameters.ordinal_position) || ');' ||
			  ' UPDATE ' || table_schema || '.' || table_name || ' SET (' || 
			  STRING_AGG(column_name , ',' ORDER BY  parameters.ordinal_position) || ') = (SELECT  '
		   ||
	   		   STRING_AGG(CASE
	   		      WHEN parameters.data_type IN ('smallint' , 'integer' , 'bigint' , 'decimal' , 'numeric' , 'real' , 'double precision' , 'smallserial' , 'serial' , 'bigserial')
	   		      THEN '11' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('money')
	   		      THEN '92.07' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('character' , 'character varying' , 'varchar' , 'char' , 'text')
	   		      THEN '''' || 2 || '''' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('bytea')
	   		      THEN 'E' || ''''|| '\\000' || '''' || '::bytea'
	   		      WHEN parameters.data_type IN ('date' , 'time with time zone' , 'time without time zone' , 'timestamp with time zone','timestamp without time zone')
	   		      THEN '''' || 'now' || '''' || '::' || parameters.data_type
	   		      WHEN parameters.data_type in ('boolean')
	   		      THEN 'false::boolean'
	   		      WHEN parameters.data_type in ('UUID')
	   		      THEN 'uuid_generate_v4 ()'
	   		      ELSE ' '
	   		      END , ',' ORDER BY parameters.ordinal_position) || ') WHERE ' || 
				  '(' || 
				  			  STRING_AGG(column_name , ',' ORDER BY  parameters.ordinal_position) || ')'
							  || ' = ' ||'( SELECT  '
		   ||
	   		   STRING_AGG(CASE
	   		      WHEN parameters.data_type IN ('smallint' , 'integer' , 'bigint' , 'decimal' , 'numeric' , 'real' , 'double precision' , 'smallserial' , 'serial' , 'bigserial')
	   		      THEN '10' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('money')
	   		      THEN '99.07' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('character' , 'character varying' , 'varchar' , 'char' , 'text')
	   		      THEN '''' || 1 || '''' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('bytea')
	   		      THEN 'E' || ''''|| '\\000' || '''' || '::bytea'
	   		      WHEN parameters.data_type IN ('date' , 'time with time zone' , 'time without time zone' , 'timestamp with time zone','timestamp without time zone')
	   		      THEN '''' || 'now' || '''' || '::' || parameters.data_type
	   		      WHEN parameters.data_type in ('boolean')
	   		      THEN 'false::boolean'
	   		      WHEN parameters.data_type in ('UUID')
	   		      THEN 'uuid_generate_v4 ()'
	   		      ELSE ' '
	   		      END , ',' ORDER BY parameters.ordinal_position) || ')'||'; ROLLBACK;'
		     FROM information_schema.columns as parameters
		    WHERE table_schema = ALIAS1.table_schema
		      AND table_name   = ALIAS1.table_name
		      and is_nullable = 'NO'
		      GROUP BY table_schema , table_name LIMIT 1
	   )
	   WHEN event_manipulation like '%DELETE%' AND event_manipulation NOT like '%UPDATE%'
 AND event_manipulation NOT like '%INSERT%' THEN
	   (
		   SELECT  'INSERT INTO ' || table_schema || '.' || table_name || '('  || 
		   	STRING_AGG(column_name , ',' ORDER BY  parameters.ordinal_position)
		   	  || ') VALUES (' ||
		   STRING_AGG(CASE
		      WHEN parameters.data_type IN ('smallint' , 'integer' , 'bigint' , 'decimal' , 'numeric' , 'real' , 'double precision' , 'smallserial' , 'serial' , 'bigserial')
		      THEN '10' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('money')
		      THEN '99.07' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('character' , 'character varying' , 'varchar' , 'char' , 'text')
		      THEN '''' || 1 || '''' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('bytea')
		      THEN 'E' || ''''|| '\\000' || '''' || '::bytea'
		      WHEN parameters.data_type IN ('date' , 'time with time zone' , 'time without time zone' , 'timestamp with time zone','timestamp without time zone')
		      THEN '''' || 'now' || '''' || '::' || parameters.data_type
		      WHEN parameters.data_type in ('boolean')
		      THEN 'true::boolean'
		      WHEN parameters.data_type in ('UUID')
		      THEN 'uuid_generate_v4 ()'
		      ELSE ' '
		      END , ',' ORDER BY parameters.ordinal_position) || ');' ||
			  ' DELETE FROM ' || table_schema || '.' || table_name ||
			 '; ROLLBACK;'
		     FROM information_schema.columns as parameters
		    WHERE table_schema = ALIAS1.table_schema
		      AND table_name   = ALIAS1.table_name
		      and is_nullable = 'NO'
		      GROUP BY table_schema , table_name LIMIT 1
	   )
	   ELSE
	   (
		   SELECT  'INSERT INTO ' || table_schema || '.' || table_name || '('  || 
		   	STRING_AGG(column_name , ',' ORDER BY  parameters.ordinal_position)
		   	  || ') VALUES (' ||
		   STRING_AGG(CASE
		      WHEN parameters.data_type IN ('smallint' , 'integer' , 'bigint' , 'decimal' , 'numeric' , 'real' , 'double precision' , 'smallserial' , 'serial' , 'bigserial')
		      THEN '10' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('money')
		      THEN '99.07' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('character' , 'character varying' , 'varchar' , 'char' , 'text')
		      THEN '''' || 1 || '''' || '::' || parameters.data_type
		      WHEN parameters.data_type IN ('bytea')
		      THEN 'E' || ''''|| '\\000' || '''' || '::bytea'
		      WHEN parameters.data_type IN ('date' , 'time with time zone' , 'time without time zone' , 'timestamp with time zone','timestamp without time zone')
		      THEN '''' || 'now' || '''' || '::' || parameters.data_type
		      WHEN parameters.data_type in ('boolean')
		      THEN 'true::boolean'
		      WHEN parameters.data_type in ('UUID')
		      THEN 'uuid_generate_v4 ()'
		      ELSE ' '
		      END , ',' ORDER BY parameters.ordinal_position) || ');' ||
			  ' UPDATE ' || table_schema || '.' || table_name || ' SET (' || 
			  STRING_AGG(column_name , ',' ORDER BY  parameters.ordinal_position) || ') = (SELECT  '
		   ||
	   		   STRING_AGG(CASE
	   		      WHEN parameters.data_type IN ('smallint' , 'integer' , 'bigint' , 'decimal' , 'numeric' , 'real' , 'double precision' , 'smallserial' , 'serial' , 'bigserial')
	   		      THEN '11' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('money')
	   		      THEN '92.07' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('character' , 'character varying' , 'varchar' , 'char' , 'text')
	   		      THEN '''' || 2 || '''' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('bytea')
	   		      THEN 'E' || ''''|| '\\000' || '''' || '::bytea'
	   		      WHEN parameters.data_type IN ('date' , 'time with time zone' , 'time without time zone' , 'timestamp with time zone','timestamp without time zone')
	   		      THEN '''' || 'now' || '''' || '::' || parameters.data_type
	   		      WHEN parameters.data_type in ('boolean')
	   		      THEN 'false::boolean'
	   		      WHEN parameters.data_type in ('UUID')
	   		      THEN 'uuid_generate_v4 ()'
	   		      ELSE ' '
	   		      END , ',' ORDER BY parameters.ordinal_position) || ') WHERE ' || 
				  '(' || 
				  			  STRING_AGG(column_name , ',' ORDER BY  parameters.ordinal_position) || ')'
							  || ' = ' ||'( SELECT  '
		   ||
	   		   STRING_AGG(CASE
	   		      WHEN parameters.data_type IN ('smallint' , 'integer' , 'bigint' , 'decimal' , 'numeric' , 'real' , 'double precision' , 'smallserial' , 'serial' , 'bigserial')
	   		      THEN '10' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('money')
	   		      THEN '99.07' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('character' , 'character varying' , 'varchar' , 'char' , 'text')
	   		      THEN '''' || 1 || '''' || '::' || parameters.data_type
	   		      WHEN parameters.data_type IN ('bytea')
	   		      THEN 'E' || ''''|| '\\000' || '''' || '::bytea'
	   		      WHEN parameters.data_type IN ('date' , 'time with time zone' , 'time without time zone' , 'timestamp with time zone','timestamp without time zone')
	   		      THEN '''' || 'now' || '''' || '::' || parameters.data_type
	   		      WHEN parameters.data_type in ('boolean')
	   		      THEN 'false::boolean'
	   		      WHEN parameters.data_type in ('UUID')
	   		      THEN 'uuid_generate_v4 ()'
	   		      ELSE ' '
	   		      END , ',' ORDER BY parameters.ordinal_position) || ')'||';' ||
			  ' DELETE FROM ' || table_schema || '.' || table_name ||
			 '; ROLLBACK;'
		     FROM information_schema.columns as parameters
		    WHERE table_schema = ALIAS1.table_schema
		      AND table_name   = ALIAS1.table_name
		      and is_nullable = 'NO'
		      GROUP BY table_schema , table_name LIMIT 1
	   )
	   
	   
	   END AS STATEMENT_SANITY from ALIAS1;
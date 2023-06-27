import warnings
import pandas as pd
import psycopg2 as pg
from urllib.parse import quote 
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')


#RECEIVE PARAMETER
param_host                      = '...'
param_port                      = '...'
param_dbname                    = '...'
param_user                      = '...' 
param_pw                        = '...'
param_schema_source             = '...'
param_schema_destination        = '...'
param_table_destination         = '...'
param_table_source_master       = '...'

date = '2022-08-10'
n = 10

print("Start on : ", param_table_destination)

query ="""
select distinct("location") as location
into temp location_ok_{b}
from data_master.v_data_quality_{b}
where "level" = 'site' and 
length(location) <> 6;

select kpi
into temp kpi_ok_{b}
from anomaly.kpi_list
where status = 'include' and 
	  rate_status = 'N' and
	  trend = 'T' and
	  category = '{b}';

delete from {a}.{b}
where kpi in (select kpi from kpi_ok_{b});

select date, kpi, granularity, node, vendor, level, location, value, 'ok' as status
from(
	select  day_name, date, kpi, granularity, node, vendor, level, location, value,
        	RANK() over (partition by day_name, kpi, granularity, node, vendor, level, location order by date desc) ranking
	from {c}.v_data_quality_{b}
	where 	date <= '{e}' and 
			value is not null and 
			value > 0 and
			kpi in (select kpi from kpi_ok_{b}) and
			"location" not in (select location from location_ok_{b}) and
			kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null
	) a
where ranking <= {n}
""".format(n=n, a=param_schema_destination, b=param_table_destination, c=param_schema_source, d=param_table_source_master, e=date)

conn_pg = pg.connect(host=param_host, port=param_port, dbname=param_dbname, user=param_user, password=param_pw)
df = pd.read_sql_query(query,conn_pg)
conn_pg.close()

#insert_into_result
con_engine = ("postgresql+psycopg2://{user}:%s@{host}:{port}/{dbname}").format(
        host = param_host, port = param_port, dbname = param_dbname, user = param_user, password = param_pw )
df.to_sql(param_table_destination, con = con_engine, if_exists = 'append', schema = param_schema_destination, index=False)
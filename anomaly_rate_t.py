import pandas as pd
import psycopg2 as pg
from urllib.parse import quote 
from sqlalchemy import create_engine
from functions import days, del_dup_r_t
import warnings
warnings.filterwarnings('ignore')

#RECEIVE PARAMETER
param_host              = 
param_port              = 
param_dbname            = 
param_user              = 
param_pw                = 
param_schema_source     = 
param_schema_dest       = 

start_date              = 
param_table_dest        = 

n = 7
ndays = 0
day = days(start_date)
param_tabel_hist        = f'h_{param_table_dest}'
param_table_source      = f'data_quality_{param_table_dest}'

con_engine = ("postgresql+psycopg2://{user}:%s@{host}:{port}/{dbname}" % quote('P@ssw0rd*123')).format(
        host = param_host, port = param_port, dbname = param_dbname, user = param_user, password = param_pw )

query0 = del_dup_r_t(param_schema_dest, param_table_dest, start_date)
connection = create_engine(con_engine).connect()
connection.execute(query0)
connection.close()

query = """
    select distinct("location") as location
    into temp location_r_t_{table_destination}
    from data_master.v_data_quality_{table_destination}
    where "level" = 'site' and 
    length(location) <> 6;

    select kpi
    into temp kpi_r_t_{table_destination}
    from anomaly.kpi_list
    where   status = 'include' and 
            rate_status = 'Y' and
            trend = 'T' and
            category = '{table_destination}';

    with data_cek as (
        select day_name, date, kpi, granularity, node, vendor, level, location, value
        from {schema_source}.v_{table_source}
        where   date = '{date}'
                and kpi in (select kpi from kpi_r_t_{table_destination})
                and "location" not in (select location from location_r_t_{table_destination})
                and kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null 
    ), data_baseline as (
        select day_name, kpi, granularity, node, vendor, level, location,
            avg(value) as rata,
            avg(value) * 1.10 as b_atas,
            avg(value) * 0.90 as b_bawah
        from (
            select  day_name, date, kpi, granularity, node, vendor, level, location, value, 
                    RANK() over (partition by day_name, kpi, granularity, node, vendor, level, location order by date desc) rank
            from (
                select * 
                from {schema_destination}.v_{table_destination}
                where 
                    status = 'ok'
                    and day_name = '{day}'
                    and date < '{date}'
                    and kpi in (select kpi from kpi_r_t_{table_destination})
                    and "location" not in (select location from location_r_t_{table_destination})
                    and kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null
                ) a
            ) b 
        where rank <= {n}
        group by day_name, kpi, granularity, node, vendor, level, location
    )
    select cek.date, cek.kpi, cek.granularity, cek.node, cek.vendor, cek.level, cek.location, cek.value, bl.b_atas, bl.b_bawah, bl.rata,
        case when cek.value is null then 'anomaly'
             when cek.value < 0 or cek.value > 100 then 'anomaly'
             when cek.value >= bl.b_bawah and cek.value <= bl.b_atas then 'ok'
             else 'anomaly' end as status,
             cek.day_name
    from data_cek cek left join data_baseline bl
    on  trim(cek.day_name) = trim(bl.day_name) and cek.kpi = bl.kpi and cek.granularity = bl.granularity and cek.node = bl.node and cek.vendor = bl.vendor and cek.level = bl.level and cek.location = bl.location
    """.format(schema_destination=param_schema_dest, table_destination=param_table_dest, n=n,
            schema_source=param_schema_source, table_source=param_table_source, date=start_date, day=day )

conn_pg = pg.connect(host=param_host, port=param_port, dbname=param_dbname, user=param_user, password=param_pw)
df = pd.read_sql_query(query,conn_pg)
conn_pg.close()

#insert_into_result
df = df.drop(['day_name'], axis=1)
df.to_sql(param_table_dest, con = con_engine, if_exists = 'append', schema = param_schema_dest, index=False)

# insert_into_history
# df = df.loc[df['status'].isin(['anomaly']) == True]
# df.to_sql(param_tabel_hist, con = con_engine, if_exists = 'append', schema = param_schema_dest, index=False)
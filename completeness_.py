import warnings
import pandas as pd
import psycopg2 as pg
from urllib.parse import quote 
from sqlalchemy import create_engine
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
param_table_source      = 
param_tabel_hist        = 

con_engine = ("postgresql+psycopg2://{user}:%s@{host}:{port}/{dbname}").format(
        host = param_host, port = param_port, dbname = param_dbname, user = param_user, password = param_pw )
n = 7
ndays = 0

query0 = """
        delete from {param_schema_dest}.{param_table_dest}
        where date = '{date}'
        """.format(param_schema_dest=param_schema_dest, param_table_dest=param_table_dest, date=start_date)
connection=create_engine(con_engine).connect()
connection.execute(query0)
connection.close()

query = """
    with data_cek as (
        select
            date,
            kpi, 
            granularity, 
            node, 
            vendor, 
            level, 
            location,
            row_count
        from {schema_source}.{table_source}
        where   date = '{date}'
                and "location" not in (select distinct("location") from {schema_source}.{table_source} where level = 'site' and length(location) <> 6)
    ), data_baseline as (
        select
            kpi, 
            granularity, 
            node, 
            vendor, 
            level, 
            location,
            avg(row_count) as rata,
            avg(row_count) * 1.08 as b_atas,
            avg(row_count) * 0.92 as b_bawah
        from (
            select 
                *, 
                RANK() over (partition by kpi, granularity, node, vendor, level, location order by date desc) rank
            from (
                select 
                    * 
                from {schema_destination}.{table_destination}
                where 
                    status = 'ok'
                    and date < '{date}'
                    and "location" not in (select distinct("location") from {schema_source}.{table_source} where level = 'site' and length(location) <> 6)
                ) a
            ) b 
        where rank <= {n}
        group by 
            kpi, 
            granularity, 
            node, 
            vendor, 
            level, 
            location
    )
    select
        cek.*,
        bl.b_atas,
        bl.b_bawah,
        bl.rata,
        case when cek.row_count >= bl.b_bawah and cek.row_count <= bl.b_atas then 'ok'
            when cek.row_count > bl.b_atas then 'duplicated'
            else 'not ok' end as status
    from data_cek cek left join data_baseline bl
    on
        cek.kpi = bl.kpi and
        cek.granularity = bl.granularity and 
        cek.node = bl.node and
        cek.vendor = bl.vendor and 
        cek.level = bl.level and
        cek.location = bl.location
    """.format(schema_destination=param_schema_dest, table_destination=param_table_dest, n=n,
            schema_source=param_schema_source, table_source=param_table_source, date=start_date )
conn_pg = pg.connect(host=param_host, port=param_port, dbname=param_dbname, user=param_user, password=param_pw)
df = pd.read_sql_query(query,conn_pg)
conn_pg.close()

#insert_into_result
df.to_sql(param_table_dest, con = con_engine, if_exists = 'append', schema = param_schema_dest, index=False)

#insert_into_history
# df = df.loc[df['status'].isin(['not ok']) == True]
# df.to_sql(param_tabel_hist, con = con_engine, if_exists = 'append', schema = param_schema_dest, index=False)
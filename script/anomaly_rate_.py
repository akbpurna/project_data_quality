import pandas as pd
from sqlalchemy import create_engine
from functions import del_dup_r, con_engine,conn_pg
import warnings
warnings.filterwarnings('ignore')

#RECEIVE PARAMETER
param_schema_source     =
param_schema_dest       =
start_date              = 
param_table_dest        = 
param_table_source      = 
param_tabel_hist        = 

n = 7
ndays = 0

query0 = del_dup_r(param_schema_dest, param_table_dest, start_date)
connection = create_engine(con_engine).connect()
connection.execute(query0)
connection.close()

query = """
    select distinct("location") as location
    into temp location_r_{table_destination}
    from data_master.v_data_quality_{table_destination}
    where "level" = 'site' and 
    length(location) <> 6;

    select kpi
    into temp kpi_r_{table_destination}
    from anomaly.kpi_list
    where   status = 'include' and 
            rate_status = 'Y' and
            trend = 'F' and
            category = '{table_destination}';

    with data_cek as (
        select date, kpi, granularity, node, vendor, level, location, value
        from {schema_source}.{table_source}
        where   date = '{date}'
                and kpi in (select kpi from kpi_r_{table_destination})
                and "location" not in (select location from location_r_{table_destination})
                and kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null 
    ), data_baseline as (
        select kpi, granularity, node, vendor, level, location,
            avg(value) as rata,
            avg(value) * 1.10 as b_atas,
            avg(value) * 0.90 as b_bawah
        from (
            select *, RANK() over (partition by kpi, granularity, node, vendor, level, location order by date desc) rank
            from (
                select * 
                from {schema_destination}.{table_destination}
                where 
                    status = 'ok'
                    and date < '{date}'
                    and kpi in (select kpi from kpi_r_{table_destination})
                    and "location" not in (select location from location_r_{table_destination})
                    and kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null
                ) a
            ) b 
        where rank <= {n}
        group by kpi, granularity, node, vendor, level, location
    )
    select cek.*, bl.b_atas, bl.b_bawah, bl.rata,
        case when cek.value is null then 'anomaly'
             when cek.value < 0 or cek.value > 100 then 'anomaly'
             when cek.value >= bl.b_bawah and cek.value <= bl.b_atas then 'ok'
             else 'anomaly' end as status
    from data_cek cek left join data_baseline bl
    on  cek.kpi = bl.kpi and cek.granularity = bl.granularity and cek.node = bl.node and cek.vendor = bl.vendor and cek.level = bl.level and cek.location = bl.location
    """.format(schema_destination=param_schema_dest, table_destination=param_table_dest, n=n,
            schema_source=param_schema_source, table_source=param_table_source, date=start_date )

df = pd.read_sql_query(query,conn_pg)
conn_pg.close()

#insert_into_result
df.to_sql(param_table_dest, con = con_engine, if_exists = 'append', schema = param_schema_dest, index=False)

# # insert_into_history
df = df.loc[df['status'].isin(['anomaly']) == True]
df.to_sql(param_tabel_hist, con = con_engine, if_exists = 'append', schema = param_schema_dest, index=False)
import warnings
import pandas as pd
import psycopg2 as pg
from dateutil import parser
from urllib.parse import quote 
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from functions  import del_dup, del_dup_r

warnings.filterwarnings('ignore')

def c_date (date1):
    date1 = parser.parse(date1)
    date2 = datetime.today()
    date2 = f'{date2.year}' + '-'+ f'{date2.month:02d}' + '-' + f'{date2.day:02d}'
    date2 = parser.parse(date2)
    diff = date2 - date1
    return diff.days
def l_time():
    date = datetime.today()
    date = f'{date.hour}' + ':'+ f'{date.minute:02d}' + ':' + f'{date.second:02d}'
    return date
def nloop(start_date, ndays):
    date_for_c = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days = ndays)
    date_for_c = f'{date_for_c.year}' + '-'+ f'{date_for_c.month:02d}' + '-' + f'{date_for_c.day:02d}'
    return date_for_c

#RECEIVE PARAMETER
param_host              = '10.54.18.24'
param_port              = '5432'
param_dbname            = 'data_quality'
param_user              = 'postgres' 
param_pw                = 'P@ssw0rd*123'
param_schema_source     = 'data_master'
param_schema_dest       = 'anomaly'

start_date              = '2022-09-21'      #parameter (tanggal yang direload) 2022-09-01
param_table_dest        = 'ran'    #parameternya (nama kategorinya)

param_table_source      = f'data_quality_{param_table_dest}'
param_tabel_hist        = f'h_{param_table_dest}'

con_engine = ("postgresql+psycopg2://{user}:%s@{host}:{port}/{dbname}" % quote('P@ssw0rd*123')).format(
        host = param_host, port = param_port, dbname = param_dbname, user = param_user, password = param_pw )
n = 7
ndays = 0
print("Start | ",param_table_dest," | ", start_date," | ", l_time())

for i in range(0, c_date(start_date)+1):
    date = nloop(start_date, ndays)

    query0 = del_dup(param_schema_dest, param_table_dest, start_date)
    connection = create_engine(con_engine).connect()
    connection.execute(query0)
    connection.close()

    query = """
        select distinct("location") as location
        into temp location_{table_destination}
        from data_master.v_data_quality_{table_destination}
        where "level" = 'site' and 
        length(location) <> 6;

        select kpi
        into temp kpi_{table_destination}
        from anomaly.kpi_list
        where   status = 'include' and 
                rate_status = 'N' and
                trend = 'F' and
                category = '{table_destination}';

        with data_cek as (
            select date, kpi, granularity, node, vendor, level, location, value
            from {schema_source}.{table_source}
            where   date = '{date}'
                    and kpi in (select kpi from kpi_{table_destination})
                    and "location" not in (select location from location_{table_destination})
                    and kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null
        ), data_baseline as (
            select kpi, granularity, node, vendor, level, location,
                avg(value) as rata,
                avg(value) * 1.1 as b_atas,
                avg(value) * 0.90 as b_bawah
            from (
                select *, RANK() over (partition by kpi, granularity, node, vendor, level, location order by date desc) rank
                from (
                    select * 
                    from {schema_destination}.{table_destination}
                    where 
                        status = 'ok'
                        and date < '{date}'
                        and kpi in (select kpi from kpi_{table_destination})
                        and "location" not in (select location from location_{table_destination})
                        and kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null
                    ) a
                ) b 
            where rank <= {n}
            group by kpi, granularity, node, vendor, level, location
        )
        select cek.*, bl.b_atas, bl.b_bawah, bl.rata,
            case when cek.value = 0 then 'anomaly'
                when cek.value is null then 'anomaly'
                when cek.value < 0 then 'anomaly'
                when cek.value >= bl.b_bawah and cek.value <= bl.b_atas then 'ok'
                else 'anomaly' end as status
        from data_cek cek left join data_baseline bl
        on  cek.kpi = bl.kpi and
            cek.granularity = bl.granularity and 
            cek.node = bl.node and
            cek.vendor = bl.vendor and 
            cek.level = bl.level and
            cek.location = bl.location,
        """.format(schema_destination=param_schema_dest, table_destination=param_table_dest, n=n,
                schema_source=param_schema_source, table_source=param_table_source, date=start_date )

    conn_pg = pg.connect(host=param_host, port=param_port, dbname=param_dbname, user=param_user, password=param_pw)
    df = pd.read_sql_query(query,conn_pg)
    conn_pg.close()

    # #insert_into_result
    # df.to_sql(param_table_dest, con = con_engine, if_exists = 'append', schema = param_schema_dest, index=False)

    # # insert_into_history
    # df = df.loc[df['status'].isin(['anomaly']) == True]
    # df.to_sql(param_tabel_hist, con = con_engine, if_exists = 'append', schema = param_schema_dest, index=False)

    print("Loop ke- ", i+1, " | ",date," | ", l_time())
    ndays += 1
from dateutil import parser
from urllib.parse import quote 
from sqlalchemy import create_engine
from datetime import datetime, timedelta

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
param_table_dest        = 'vas' #parameternya (nama kategorinya)
param_schema_dest       = 'completeness'
param_table_source      = 'data_quality_{}'.format(param_table_dest)
param_schema_source     = 'data_master'

# #CONNECT TO DB
con_engine = ("postgresql+psycopg2://{user}:%s@{host}:{port}/{dbname}" % quote('P@ssw0rd*123')).format(
        host = param_host, port = param_port, dbname = param_dbname, user = param_user, password = param_pw )
n = 7
ndays = 0
start_date = '2022-09-07' #parameter (tanggal yang direload)
print("Start: ",param_table_dest.title()," | ", l_time())

for i in range(0, c_date(start_date)+1):
    date = nloop(start_date, ndays)
    query = """
    delete from {schema_destination}.{table_destination}
    where date = '{date}';
    
    with data_cek as ( -- data yang mau di bandingkan
        select
            date,
            kpi, 
            granularity, 
            node, 
            vendor, 
            level, 
            location,
            row_count
        from {schema_source}.{table_source} -- nama tabel masternya
        where date = '{date}'
    ), data_baseline as ( -- data baselinenya
        select
            kpi, 
            granularity, 
            node, 
            vendor, 
            level, 
            location,
            avg(row_count) as rata,
            avg(row_count) * 1.05 as b_atas,
            avg(row_count) * 0.95 as b_bawah
        from (
            select 
                *, 
                RANK() over (partition by kpi, granularity, node, vendor, level, location order by date desc) rank
            from (
                select 
                    * 
                from {schema_destination}.{table_destination}  -- tabel hasil | ganti tabelnya kalau udah beres semua
                where 
                    status = 'ok'
                    and date < '{date}'
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
    insert into {schema_destination}.{table_destination} (date,kpi,granularity,node,vendor,"level","location",row_count,b_atas,b_bawah,rata,status)
    select
        cek.*,
        bl.b_atas,
        bl.b_bawah,
        bl.rata,
        case when cek.row_count >= bl.b_bawah and cek.row_count <= bl.b_atas then 'ok'
            when cek.row_count > bl.b_atas then 'duplicated'
            else 'not ok' end as status
    --into {schema_destination}.{table_destination}
    from data_cek cek left join data_baseline bl
    on
        cek.kpi = bl.kpi and
        cek.granularity = bl.granularity and 
        cek.node = bl.node and
        cek.vendor = bl.vendor and 
        cek.level = bl.level and
        cek.location = bl.location

    """.format(schema_destination=param_schema_dest, table_destination=param_table_dest, schema_source=param_schema_source, table_source=param_table_source, date=date, n=n)
    connection = create_engine(con_engine).connect()
    connection.execute(query)
    connection.close()
    print("Loop ke- ", i+1, " | ",date," | ", l_time())
    ndays += 1

print("DONE")
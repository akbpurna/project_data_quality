import warnings
import pandas as pd
import psycopg2 as pg
from dateutil import parser
from urllib.parse import quote 
from sqlalchemy import create_engine
from datetime import datetime, timedelta
warnings.filterwarnings('ignore')

#RECEIVE PARAMETER
param_host                  = '...'
param_port                  = '...'
param_dbname                = '...'
param_user                  = '...' 
param_pw                    = '...'
param_schema_source         = '...'
param_schema_destination    = '...'

param_kpi                   = '...'         #parameter          yg dikirim dari tabel interface
param_node                  = '...'         #parameter          yg dikirim dari tabel interface
param_level                 = '...'          #parameter          yg dikirim dari tabel interface
param_vendor                = '...'       #parameter          yg dikirim dari tabel interface
param_granularity           = '...'       #parameter          yg dikirim dari tabel interface
param_loc                   = '...'   #parameter location yg dikirim dari tabel interface
start_date                  = '...'  #parameter tanggal  yg dikirim dari tabel interface
param_table_destination     = '...'        #parameter categori yg dikirim dari tabel interface

param_table_source          = '...'
con_engine = ("postgresql+psycopg2://{user}:%s@{host}:{port}/{dbname}").format(
    host = param_host, port = param_port, dbname = param_dbname, user = param_user, password = param_pw )

n = 7

conn_pg = pg.connect(host=param_host, port=param_port, dbname=param_dbname, user=param_user, password=param_pw)

#query cek axisting data
query0 = """SELECT EXISTS(
		SELECT * 
		FROM {s_source}.{t_source} 
		WHERE 
			date 			= '{date}'
		    and kpi         = '{kpi}'
		    and granularity = '{grad}'
		    and node        = '{node}'
		    and vendor      = '{vend}'
		    and level       = '{lev}'
		    and location    = '{loc}')::INT as hasil
            """.format( 
                        date        = start_date,
                        kpi         = param_kpi,
                        loc         = param_loc,
                        node        = param_node,
                        lev         = param_level,
                        vend        = param_vendor,
                        grad        = param_granularity,
                        t_source    = param_table_source,
                        s_source    = param_schema_source,
                        t_dest      = param_table_destination,
                        s_dest      = param_schema_destination
                        )
exist = pd.read_sql_query(query0,conn_pg)
exist = exist['hasil'][0]

# query menghitung jumlah row yang ok (basel)
query1 = """
        select count(*) as jml
        from {s_dest}.{t_dest}
        where   kpi             = '{kpi}'
                and granularity = '{grad}'
                and vendor      = '{vend}'
                and node        = '{node}'
                and level       = '{lev}'
                and location    = '{loc}'
                and status      = 'ok'
        """.format( 
                kpi         = param_kpi,
                loc         = param_loc,
                node        = param_node,
                lev         = param_level,
                vend        = param_vendor,
                grad        = param_granularity,
                t_dest      = param_table_destination,
                s_dest      = param_schema_destination
                )
basel = pd.read_sql_query(query1,conn_pg)
basel = basel['jml'][0]
conn_pg.close()

# query hitung ok > 7 row
query2 = """
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
                from {s_source}.{t_source}
                where date          = '{date}'
                    and kpi         = '{kpi}'
                    and granularity = '{grad}'
                    and vendor      = '{vend}'
                    and node        = '{node}'
                    and level       = '{lev}'
                    and location    = '{loc}'
            ), data_baseline as (
            select
                kpi, 
                granularity, 
                node, 
                vendor, 
                level, 
                location,
                avg(row_count::float) as rata,
                0.95 * avg(row_count::float) as b_bawah,	
                1.05 * avg(row_count::float) as b_atas
            from (
                select 
                    *, 
                    RANK() over (order by date desc) rank
                from (
                    select 
                        distinct date, kpi, granularity, node, vendor, "level", "location", row_count, b_bawah, rata, b_atas, status 
                    from {s_dest}.{t_dest}
                    where   date            < '{date}'
                            and kpi         = '{kpi}'
                            and granularity = '{grad}'
                            and vendor      = '{vend}'
                            and node        = '{node}'
                            and level       = '{lev}'
                            and location    = '{loc}'
                            and status      = 'ok'
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
            --insert into {s_dest}.{t_dest} (date,kpi,granularity,node,vendor,"level","location",row_count,b_atas,b_bawah,rata,status)
            select
                cek.*,
                bl.b_atas,
                bl.b_bawah,
                bl.rata,
                case    when cek.row_count >= bl.b_bawah and cek.row_count <= bl.b_atas then 'ok'
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

            """.format( 
                        n           = n,
                        date        = start_date,
                        kpi         = param_kpi,
                        loc         = param_loc,
                        node        = param_node,
                        lev         = param_level,
                        vend        = param_vendor,
                        grad        = param_granularity,
                        t_source    = param_table_source,
                        s_source    = param_schema_source,
                        t_dest      = param_table_destination,
                        s_dest      = param_schema_destination
                        )

# query no data
query3 = """
    with data_baseline as (
        select
            kpi, 
            granularity, 
            node, 
            vendor, 
            level, 
            location,
            avg(row_count::float) as rata,
            (0.95 * avg(row_count::float)) as b_bawah,	
            (1.05 * avg(row_count::float)) as b_atas
        from (
            select 
                *, 
                RANK() over (order by date desc) rank
            from (
                select 
                    distinct date, kpi, granularity, node, vendor, "level", "location", row_count, b_bawah, rata, b_atas, status  
                from {s_dest}.{t_dest}
                where   date            < '{date}'
                        and kpi         = '{kpi}'
                        and granularity = '{grad}'
                        and vendor      = '{vend}'
                        and node        = '{node}'
                        and level       = '{lev}'
                        and location    = '{loc}'
                        and status      = 'ok'
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
    --insert into {s_dest}.{t_dest} (date, kpi, granularity, node, vendor, "level", "location", row_count, b_bawah, rata, b_atas, status)
    select 
        '{date}'::date,
        kpi,
        granularity,
        node,
        vendor,
        "level",
        "location",
        null as row_count,
        b_bawah,
        rata,
        b_atas,
        'no data' as status
	from data_baseline
	where 	kpi             = '{kpi}'
            and granularity = '{grad}'
            and vendor      = '{vend}'
            and node        = '{node}'
            and level       = '{lev}'
            and location    = '{loc}'
            """.format( 
                        n = n, date = start_date, kpi = param_kpi, loc = param_loc, node = param_node, lev = param_level,
                        vend = param_vendor, grad = param_granularity, t_dest = param_table_destination, s_dest = param_schema_destination
                        )

# query hapus duplikasi
query4 = """
delete from {s_dest}.{t_dest}
where   date            = '{date}'
        and kpi         = '{kpi}'
        and granularity = '{grad}'
        and vendor      = '{vend}'
        and node        = '{node}'
        and level       = '{lev}'
        and location    = '{loc}'
""".format( 
            date   = start_date,
            kpi    = param_kpi,
            loc    = param_loc,
            node   = param_node,
            lev    = param_level,
            vend   = param_vendor,
            grad   = param_granularity,
            t_dest = param_table_destination,
            s_dest = param_schema_destination
            )
connection = create_engine(con_engine).connect()
connection.execute(query4)
connection.close()

# query membuat baseline
query5 = """
        select date, kpi, granularity, node, vendor, "level", "location", row_count, 'ok' as status
        from {s_source}.{t_source}
        where   date            = '{date}'
                and kpi         = '{kpi}'
                and granularity = '{grad}'
                and vendor      = '{vend}'
                and node        = '{node}'
                and level       = '{lev}'
                and location    = '{loc}'
                and row_count is not null
        """.format( 
                    date        = start_date,
                    kpi         = param_kpi,
                    loc         = param_loc,
                    node        = param_node,
                    lev         = param_level,
                    vend        = param_vendor,
                    grad        = param_granularity,
                    t_source    = param_table_source,
                    s_source    = param_schema_source,
                    t_dest      = param_table_destination,
                    s_dest      = param_schema_destination
                    )
conn_pg = pg.connect(host=param_host, port=param_port, dbname=param_dbname, user=param_user, password=param_pw)

if exist == 1: # jika data exist?
    if basel < 7:
        inisiasi = pd.read_sql_query(query5,conn_pg)
        inisiasi.to_sql(param_table_destination, con = con_engine, if_exists = 'append', schema = param_schema_destination, index=False)
    else:
        df = pd.read_sql_query(query2,conn_pg)
        df.to_sql(param_table_destination, con = con_engine, if_exists = 'append', schema = param_schema_destination, index=False)
else:
    df = pd.read_sql_query(query3,conn_pg)
    df.to_sql(param_table_destination, con = con_engine, if_exists = 'append', schema = param_schema_destination, index=False)
conn_pg.close()
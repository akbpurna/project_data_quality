from parameters import *

def a_ok_t(category:str, date_:str):
    '''category : 'core', date : '2022-04-21' '''
    param_table_destination         = category #cukup ganti ini saja, nanti dibuat dinamis berdasarkan pemanggilan
    param_table_source_master       = 'data_quality_{}'.format(param_table_destination) #ganti nama tabelnya
    date = date_

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
    where ranking <= {n},
    """.format(n=n, a=param_schema_destination, b=param_table_destination, c=param_schema_source, d=param_table_source_master, e=date)

    return query

def a_ok_nt(category:str, date_:str):
    '''category : 'core', date : '2022-04-21' '''
    param_table_destination         = category #cukup ganti ini saja, nanti dibuat dinamis berdasarkan pemanggilan
    param_table_source_master       = 'data_quality_{}'.format(param_table_destination) #ganti nama tabelnya
    date = date_

    query ="""
    select distinct("location") as location
    into temp location_
    from data_master.v_data_quality_{b}
    where "level" = 'site' and 
    length(location) = 6;

    select kpi
    into temp kpi_
    from anomaly.kpi_list
    where status = 'include' and 
        rate_status = 'N' and
        trend = 'F' and
        category = '{b}';

    delete from {a}.{b}
    where kpi in (select kpi from kpi_);

    select date, kpi, granularity, node, vendor, level, location, value, 'ok' as status
    from(
        select  date, kpi, granularity, node, vendor, level, location, value,
                RANK() over (partition by kpi, granularity, node, vendor, level, location order by date desc) ranking
        from {c}.v_data_quality_{b}
        where 	date <= '{e}' and 
                value is not null and 
                value > 0 and
                kpi in (select kpi from kpi_) and
                "location" not in (select location from location_) and
                kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null
        ) a
    where ranking <= {n}
    """.format(n=n, a=param_schema_destination, b=param_table_destination, c=param_schema_source, d=param_table_source_master, e=date)
    return query

def a_ok_r_t(category:str, date_:str):
    '''category : 'core', date : '2022-04-21' '''
    param_table_destination         = category #cukup ganti ini saja, nanti dibuat dinamis berdasarkan pemanggilan
    param_table_source_master       = 'data_quality_{}'.format(param_table_destination) #ganti nama tabelnya
    date = date_

    query ="""
    select distinct("location") as location
    into temp location_
    from data_master.v_data_quality_{b}
    where "level" = 'site' and 
    length(location) = 6;

    select kpi
    into temp kpi_
    from anomaly.kpi_list
    where status = 'include' and 
        rate_status = 'Y' and
        trend = 'T' and
        category = '{b}';

    delete from {a}.{b}
    where kpi in (select kpi from kpi_);

    select date, kpi, granularity, node, vendor, level, location, value, 'ok' as status
    from(
        select  day_name, date, kpi, granularity, node, vendor, level, location, value,
                RANK() over (partition by day_name, kpi, granularity, node, vendor, level, location order by date desc) ranking
        from {c}.v_data_quality_{b}
        where 	date <= '{e}' and 
                    value is not null and 
                    value >= 0 and
                    value <= 100 and
                    kpi in (select kpi from kpi_) and
                    "location" not in (select location from location_) and
                    kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null
        ) a
    where ranking <= {n}
    """.format(n=n, a=param_schema_destination, b=param_table_destination, c=param_schema_source, d=param_table_source_master, e=date)
    return query

def a_ok_r_nt(category:str, date_:str):
    '''category : 'core', date : '2022-04-21' '''
    param_table_destination         = category #cukup ganti ini saja, nanti dibuat dinamis berdasarkan pemanggilan
    param_table_source_master       = 'data_quality_{}'.format(param_table_destination) #ganti nama tabelnya
    date = date_

    query ="""
    select distinct("location") as location
    into temp location_
    from data_master.v_data_quality_{b}
    where "level" = 'site' and 
    length(location) = 6;

    select kpi
    into temp kpi_
    from anomaly.kpi_list
    where status = 'include' and 
        rate_status = 'Y' and
        trend = 'F' and
        category = '{b}';

    delete from {a}.{b}
    where kpi in (select kpi from kpi_);

    select date, kpi, granularity, node, vendor, level, location, value, 'ok' as status
    from(
        select  date, kpi, granularity, node, vendor, level, location, value,
                RANK() over (partition by kpi, granularity, node, vendor, level, location order by date desc) ranking
        from {c}.v_data_quality_{b}
        where 	date <= '{e}' and 
                    value is not null and 
                    value >= 0 and
                    value <= 100 and
                    kpi in (select kpi from kpi_) and
                    "location" not in (select location from location_) and
                    kpi is not null and granularity is not null and node is not null and "level" is not null and "location" is not null
        ) a
    where ranking <= {n}
    """.format(n=n, a=param_schema_destination, b=param_table_destination, c=param_schema_source, d=param_table_source_master, e=date)
    return query, param_table_source_master

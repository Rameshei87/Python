\o /home/ramesh/Automation/reports/SI_bts_unique_users_report_MTD.csv
select SITE_NAME_,BSC_NAME_,
count( distinct case when month_name = '2020-01' then MSISDN end ) JAN_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-02' then MSISDN end ) FEB_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-03' then MSISDN end ) MAR_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-04' then MSISDN end ) APR_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-05' then MSISDN end ) MAY_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-06' then MSISDN end ) JUN_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-07' then MSISDN end ) JUL_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-08' then MSISDN end ) AUG_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-09' then MSISDN end ) SEP_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-10' then MSISDN end ) OCT_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-11' then MSISDN end ) NOV_2020_ACTIVE_USERS,
count( distinct case when month_name = '2020-12' then MSISDN end ) DEC_2020_ACTIVE_USERS,
count( distinct case when month_name = '2021-01' then MSISDN end ) JAN_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-02' then MSISDN end ) FEB_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-03' then MSISDN end ) MAR_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-04' then MSISDN end ) APR_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-05' then MSISDN end ) MAY_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-06' then MSISDN end ) JUN_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-07' then MSISDN end ) JUL_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-08' then MSISDN end ) AUG_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-09' then MSISDN end ) SEP_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-10' then MSISDN end ) OCT_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-11' then MSISDN end ) NOV_2021_ACTIVE_USERS,
count( distinct case when month_name = '2021-12' then MSISDN end ) DEC_2021_ACTIVE_USERS
from (select distinct SITE_NAME SITE_NAME_,BSC_NAME BSC_NAME_,pri_identity MSISDN,to_char(CUST_LOCAL_END_DATE,'YYYY-MM') month_name
from report.dw_cell_id_2017_v2 a,cbs_cdr_voice b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and CUST_LOCAL_END_DATE >= '01-Jan-2020'
union
select distinct SITE_NAME,BSC_NAME,pri_identity,to_char(CUST_LOCAL_END_DATE,'YYYY-MM') month_
from report.dw_cell_id_2017_v2 a,cbs_cdr_data b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and CUST_LOCAL_END_DATE >= '01-Jan-2020'
union
select distinct SITE_NAME,BSC_NAME,pri_identity,to_char(CUST_LOCAL_END_DATE,'YYYY-MM') month_
from report.dw_cell_id_2017_v3 a,cbs_cdr_data b
where a.cell_id_hex = upper(substr(Calling_Cell_ID,13,4))
and a.lac_hex = upper(substr(Calling_Cell_ID,9,4))
and CUST_LOCAL_END_DATE >= '01-Jan-2021'
and length(Calling_Cell_ID) = '16'
union
select distinct SITE_NAME,BSC_NAME,pri_identity,to_char(CUST_LOCAL_END_DATE,'YYYY-MM') month_
from report.dw_cell_id_2020_4g a,cbs_cdr_data b
where a.cell_id_hex = upper(substr(Calling_Cell_ID,25,2))
and a.enodeb_hex = upper(substr(Calling_Cell_ID,21,4))
and CUST_LOCAL_END_DATE >= '01-Jan-2021'
and length(Calling_Cell_ID) = '26'
) n1
group by site_name_,BSC_NAME_ ;

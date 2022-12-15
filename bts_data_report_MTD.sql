\o /home/ramesh/Automation/reports/SI_bts_2g_3g_data_usage_MTD.csv
select SITE_NAME,BSC_NAME,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-01' then rate_usage end )/(1024*1024)) JAN_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-02' then rate_usage end )/(1024*1024)) FEB_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-03' then rate_usage end )/(1024*1024)) MAR_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-04' then rate_usage end )/(1024*1024)) APR_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-05' then rate_usage end )/(1024*1024)) MAY_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-06' then rate_usage end )/(1024*1024)) JUN_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-07' then rate_usage end )/(1024*1024)) JUL_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-08' then rate_usage end )/(1024*1024)) AUG_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-09' then rate_usage end )/(1024*1024)) SEP_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-10' then rate_usage end )/(1024*1024)) OCT_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-11' then rate_usage end )/(1024*1024)) NOV_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-12' then rate_usage end )/(1024*1024)) DEC_2021_DATA_USAGE_MB
from report.dw_cell_id_2017_v3 a,cbs_cdr_data b
where a.cell_id_hex = upper(substr(Calling_Cell_ID,13,4))
and a.lac_hex = upper(substr(Calling_Cell_ID,9,4))
and CUST_LOCAL_END_DATE >= '01-Jan-2021'
and length(Calling_Cell_ID) = '16'
group by SITE_NAME,BSC_NAME
order by SITE_NAME,BSC_NAME;

\o /home/ramesh/Automation/reports/SI_bts_4g_data_usage_MTD.csv
select SITE_NAME,BSC_NAME,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-01' then rate_usage end )/(1024*1024)) JAN_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-02' then rate_usage end )/(1024*1024)) FEB_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-03' then rate_usage end )/(1024*1024)) MAR_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-04' then rate_usage end )/(1024*1024)) APR_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-05' then rate_usage end )/(1024*1024)) MAY_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-06' then rate_usage end )/(1024*1024)) JUN_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-07' then rate_usage end )/(1024*1024)) JUL_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-08' then rate_usage end )/(1024*1024)) AUG_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-09' then rate_usage end )/(1024*1024)) SEP_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-10' then rate_usage end )/(1024*1024)) OCT_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-11' then rate_usage end )/(1024*1024)) NOV_2021_DATA_USAGE_MB,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-12' then rate_usage end )/(1024*1024)) DEC_2021_DATA_USAGE_MB
from report.dw_cell_id_2020_4g a,cbs_cdr_data b
where a.cell_id_hex = upper(substr(Calling_Cell_ID,25,2))
and a.enodeb_hex = upper(substr(Calling_Cell_ID,21,4))
and CUST_LOCAL_END_DATE >= '01-Jan-2021'
and length(Calling_Cell_ID) = '26'
group by SITE_NAME,BSC_NAME
order by SITE_NAME,BSC_NAME;
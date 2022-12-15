\o /home/ramesh/Automation/reports/SI_bts_mou_voice_MTD.csv
select SITE_NAME,BSC_NAME,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-01' then rate_usage end )/60) JAN_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-02' then rate_usage end )/60) FEB_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-03' then rate_usage end )/60) MAR_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-04' then rate_usage end )/60) APR_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-05' then rate_usage end )/60) MAY_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-06' then rate_usage end )/60) JUN_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-07' then rate_usage end )/60) JUL_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-08' then rate_usage end )/60) AUG_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-09' then rate_usage end )/60) SEP_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-10' then rate_usage end )/60) OCT_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-11' then rate_usage end )/60) NOV_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2020-12' then rate_usage end )/60) DEC_2020_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-01' then rate_usage end )/60) JAN_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-02' then rate_usage end )/60) FEB_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-03' then rate_usage end )/60) MAR_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-04' then rate_usage end )/60) APR_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-05' then rate_usage end )/60) MAY_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-06' then rate_usage end )/60) JUN_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-07' then rate_usage end )/60) JUL_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-08' then rate_usage end )/60) AUG_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-09' then rate_usage end )/60) SEP_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-10' then rate_usage end )/60) OCT_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-11' then rate_usage end )/60) NOV_2021_MOU,
round(sum( case when to_char(CUST_LOCAL_END_DATE,'YYYY-MM') = '2021-12' then rate_usage end )/60) DEC_2021_MOU
from report.dw_cell_id_2017_v2 a,cbs_cdr_voice b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and CUST_LOCAL_END_DATE >= '01-Jan-2020'
group by SITE_NAME,BSC_NAME
order by SITE_NAME,BSC_NAME;



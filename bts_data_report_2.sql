\o /home/ramesh/SI_MAL207_MAL210_bts_usage_data.csv
select to_char(CUST_LOCAL_END_DATE, 'YYYY-MM') date_,bsc_name,region,round(sum(actual_usage)/(1024*1024)) Data_usage_MB,count(distinct pri_identity) Unique_users,sum((round(DEBIT_FROM_ADVANCE_PREPAID))/10000) revenue
from report.dw_cell_id_2017_v2 a,cbs_cdr_data b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and bsc_name in ('MAL207','MAL210')
and to_char(CUST_LOCAL_END_DATE,'YYYY-MM') >= '01-Jan-2018'
--and to_char(end_DATE,'YYYY-MM-DD') >= '2017-AUG-01'
group by to_char(CUST_LOCAL_END_DATE, 'YYYY-MM'),bsc_name,region --bsc_name
order by 1, to_char(CUST_LOCAL_END_DATE, 'YYYY-MM'),bsc_name,region ;-- bsc_name;


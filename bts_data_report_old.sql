\o /home/ramesh/Automation/reports/SI_bts_data_usage_old_v3.csv
select to_char(end_date, 'YYYY-MM') date_,bsc_name,region,round(sum(actual_usage)/(1024*1024)) Data_usage_MB,count(distinct pri_identity) Unique_users
from report.dw_cell_id_2017 a,cbs_cdr_data b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
--and bsc_name = 'WES208'
--and to_char(end_DATE,'YYYY-MM-DD') >= to_char(CURRENT_DATE-20,'YYYY-MM-DD')
and end_DATE >= '01-Jul-2017'
--and to_char(end_DATE,'YYYY-MM-DD') >= '2017-AUG-01'
group by to_char(end_date, 'YYYY-MM'),bsc_name,region --bsc_name
order by 1, to_char(end_date, 'YYYY-MM'),bsc_name,region ;-- bsc_name;


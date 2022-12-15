\o /home/ramesh/Automation/reports/SI_bts_mou_voice_old_v3.csv
select to_char(end_date, 'YYYY-MM') date_,bsc_name,region,round(sum(actual_usage)/60) MOU,count(distinct pri_identity) Unique_users
from report.dw_cell_id_2017_v2 a,cbs_cdr_voice b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and Service_Flow = '1'
--and bsc_name = 'WES208'
--and to_char(end_DATE,'YYYY-MM-DD') >= to_char(CURRENT_DATE-20,'YYYY-MM-DD') 
--and to_char(end_DATE,'YYYY-MM-DD') >= '2017-AUG-01'
and end_DATE >= '01-Jul-2017'
group by to_char(end_date, 'YYYY-MM') ,bsc_name,region
order by 1,bsc_name,region;


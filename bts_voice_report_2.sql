\o /home/ramesh/Automation/reports/WES101_WES209_voice_users.csv
select distinct pri_identity Unique_users
from report.dw_cell_id_2017_v2 a,cbs_cdr_voice b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and Service_Flow = '1'
and bsc_name in ('WES101','WES209')
and to_char(start_date,'YYYY-MM-DD') >= to_char(CURRENT_DATE-60,'YYYY-MM-DD') 
--and to_char(end_DATE,'YYYY-MM-DD') >= '2017-AUG-01'
order by 1;


\o /home/ramesh/WES101_WES209_users.csv
select distinct pri_identity Unique_users
from report.dw_cell_id_2017_v2 a,cbs_cdr_voice b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and Service_Flow = '1'
and bsc_name in (
'WES101','WES209')
and start_date >= '01-Apr-2020'
union
select distinct pri_identity
from report.dw_cell_id_2017_v2 a,cbs_cdr_data b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and bsc_name in (
'WES101','WES209'
)
and start_date >= '01-Apr-2020';


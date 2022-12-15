\o /home/ramesh/Automation/reports/honiara_voice_users_v2.csv
select distinct pri_identity Unique_users
from report.dw_cell_id_2017_v2 a,cbs_cdr_voice b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and Service_Flow = '1'
and cell_name in (
'MUN101-1',
'MUN101-2',
'MUN101-3',
'WES101-1',
'WES101-2',
'WES101-3',
'MUN201-1',
'MUN201-2',
'MUN201-3',
'WES202-1',
'WES202-2',
'WES202-3',
'WES203-1',
'WES203-2',
'WES203-3',
'WES204-1',
'WES204-2',
'WES204-3',
'WES205-1',
'WES205-2',
'WES205-3',
'WES206-1',
'WES206-2',
'WES206-3',
'WES103-1',
'WES103-2',
'WES103-3',
'WES209-1',
'WES209-2',
'WES209-3',
'WES210-1',
'WES210-2',
'WES210-3'
)
and start_date >= '01-Feb-2020';
order by 1;


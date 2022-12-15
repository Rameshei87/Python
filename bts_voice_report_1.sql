\o /home/ramesh/SI_MAL207_MAL210_bts_mou_voice.csv
select to_char(CUST_LOCAL_END_DATE, 'YYYY-MM') date_,bsc_name,region,round(sum(actual_usage)/60) MOU,count(distinct pri_identity) Unique_users,sum((round(DEBIT_FROM_ADVANCE_PREPAID))/10000) revenue
from report.dw_cell_id_2017_v2 a,cbs_cdr_voice b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and Service_Flow = '1'
and bsc_name in ('MAL207','MAL210')
and CUST_LOCAL_END_DATE >= '01-Jan-2018' 
group by to_char(CUST_LOCAL_END_DATE, 'YYYY-MM') ,bsc_name,region
order by 1,bsc_name,region;


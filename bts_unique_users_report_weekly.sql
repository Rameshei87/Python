\o /home/ramesh/Automation/reports/SI_bts_unique_users_report_weekly.csv
select SITE_NAME_,BSC_NAME_,
count( distinct case when WEEK_NUMBER =1 then MSISDN end ) WEEK_1_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =2 then MSISDN end ) WEEK_2_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =3 then MSISDN end ) WEEK_3_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =4 then MSISDN end ) WEEK_4_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =5 then MSISDN end ) WEEK_5_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =6 then MSISDN end ) WEEK_6_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =7 then MSISDN end ) WEEK_7_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =8 then MSISDN end ) WEEK_8_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =9 then MSISDN end ) WEEK_9_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =10 then MSISDN end ) WEEK_10_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =11 then MSISDN end ) WEEK_11_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =12 then MSISDN end ) WEEK_12_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =13 then MSISDN end ) WEEK_13_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =14 then MSISDN end ) WEEK_14_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =15 then MSISDN end ) WEEK_15_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =16 then MSISDN end ) WEEK_16_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =17 then MSISDN end ) WEEK_17_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =18 then MSISDN end ) WEEK_18_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =19 then MSISDN end ) WEEK_19_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =20 then MSISDN end ) WEEK_20_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =21 then MSISDN end ) WEEK_21_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =22 then MSISDN end ) WEEK_22_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =23 then MSISDN end ) WEEK_23_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =24 then MSISDN end ) WEEK_24_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =25 then MSISDN end ) WEEK_25_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =26 then MSISDN end ) WEEK_26_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =27 then MSISDN end ) WEEK_27_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =28 then MSISDN end ) WEEK_28_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =29 then MSISDN end ) WEEK_29_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =30 then MSISDN end ) WEEK_30_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =31 then MSISDN end ) WEEK_31_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =32 then MSISDN end ) WEEK_32_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =33 then MSISDN end ) WEEK_33_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =34 then MSISDN end ) WEEK_34_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =35 then MSISDN end ) WEEK_35_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =36 then MSISDN end ) WEEK_36_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =37 then MSISDN end ) WEEK_37_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =38 then MSISDN end ) WEEK_38_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =39 then MSISDN end ) WEEK_39_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =40 then MSISDN end ) WEEK_40_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =41 then MSISDN end ) WEEK_41_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =42 then MSISDN end ) WEEK_42_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =43 then MSISDN end ) WEEK_43_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =44 then MSISDN end ) WEEK_44_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =45 then MSISDN end ) WEEK_45_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =46 then MSISDN end ) WEEK_46_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =47 then MSISDN end ) WEEK_47_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =48 then MSISDN end ) WEEK_48_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =49 then MSISDN end ) WEEK_49_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =50 then MSISDN end ) WEEK_50_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =51 then MSISDN end ) WEEK_51_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =52 then MSISDN end ) WEEK_52_ACTIVE_USERS,
count( distinct case when WEEK_NUMBER =53 then MSISDN end ) WEEK_53_ACTIVE_USERS
from (select distinct SITE_NAME SITE_NAME_,BSC_NAME BSC_NAME_,pri_identity MSISDN,EXTRACT(week FROM CUST_LOCAL_END_DATE) WEEK_NUMBER
from report.dw_cell_id_2017_v2 a,cbs_cdr_voice b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and CUST_LOCAL_END_DATE >= '01-Jan-2021'
union
select distinct SITE_NAME,BSC_NAME,pri_identity,EXTRACT(week FROM CUST_LOCAL_END_DATE) WEEK_NUMBER
from report.dw_cell_id_2017_v2 a,cbs_cdr_data b
where a.cell_id = substr(b.Calling_Cell_ID,11,5)
and a.lac = substr(b.Calling_Cell_ID,6,5)
and CUST_LOCAL_END_DATE >= '01-Jan-2011'
union
select distinct SITE_NAME,BSC_NAME,pri_identity,EXTRACT(week FROM CUST_LOCAL_END_DATE) WEEK_NUMBER
from report.dw_cell_id_2017_v3 a,cbs_cdr_data b
where a.cell_id_hex = upper(substr(Calling_Cell_ID,13,4))
and a.lac_hex = upper(substr(Calling_Cell_ID,9,4))
and CUST_LOCAL_END_DATE >= '01-Jan-2021'
and length(Calling_Cell_ID) = '16'
UNION
select distinct SITE_NAME,BSC_NAME,pri_identity,EXTRACT(week FROM CUST_LOCAL_END_DATE) WEEK_NUMBER 
from report.dw_cell_id_2020_4g a,cbs_cdr_data b
where a.cell_id_hex = upper(substr(Calling_Cell_ID,25,2))
and a.enodeb_hex = upper(substr(Calling_Cell_ID,21,4))
and CUST_LOCAL_END_DATE >= '01-Jan-2021'
and length(Calling_Cell_ID) = '26') n1
group by site_name_,BSC_NAME_ ;

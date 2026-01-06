/*with converted_id as (
    select
    converted_opportunity_id
    from demo_db.gtm_case.leads
    where converted_opportunity_id is not null
),
opportunities_data as (
    select
    *
    from demo_db.gtm_case.opportunities
    where opportunity_id in (select * from converted_id)
)
select
how_did_you_hear_about_us_c,
count(*)
from opportunities_data
group by 1
order by 2 desc


select
case
when (online_ordering_used in ('['''']', 'nan', '[]') or online_ordering_used is null)
and (marketplaces_used in ('['''']', 'nan', '[]') or marketplaces_used is null)
then 'no online used' else 'online used' end as online_status,
case when converted_opportunity_id is not null then 'converted' else 'not converted' end as converted,
count(*)
from demo_db.gtm_case.leads
where online_ordering_used != 'nan' or marketplaces_used != 'nan'
group by 1,2
order by 1,2


select
status,
--case when converted_opportunity_id is not null then 'converted' else 'not converted' end as converted,
sum(REPLACE(predicted_sales_with_owner, ',', '.')::FLOAT) predicted_sales
from demo_db.gtm_case.leads
group by 1--,2
order by 1--,2


select
case when location_count between 1 and 10 then '1-10'
when location_count between 11 and 25 then '11-25'
when location_count between 26 and 100 then '26-100'
when location_count > 100 then 'over 100'
else 'unknown' end as location_count_bucket,
status,
count(*),
sum(REPLACE(predicted_sales_with_owner, ',', '.')::FLOAT) predicted_sales
from demo_db.gtm_case.leads
group by 1,2
order by 1,2

select
DATE_TRUNC('MONTH', close_date) as month,
source_type,
sum(predicted_sales) predicted_sales
from demo_db.de_case_rootxv_schema.lead_and_opportunity_funnel_t
where close_date is not null
and stage_name in ('Closed Won', 'Verbal Commitment')
group by 1,2

select * from demo_db.de_case_rootxv_schema.cost_and_sales_t
order by month, source_type
*/
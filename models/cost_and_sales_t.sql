{{
    config(
    materialized='table'
    )
}}
--want to create a simplified look at the monthly closed opps vs the cost
--this will give us some insight into ROI between teams
--idea is that sales leads can look at performance month to month
with cost as (
    select
    month,
    'Inbound' as source_type,
    total_inbound_cost as cost
    from {{ ref('combined_cost_v')}}

    union all

    select
    month,
    'Outbound' as source_type,
    outbound_sales_salary as cost
    from {{ ref('combined_cost_v')}}
),
sales as (
    select
    DATE_TRUNC('MONTH', close_date) as month,
    source_type,
    sum(predicted_sales) predicted_sales
    from {{ ref('lead_and_opportunity_funnel_t')}}
    where close_date is not null
    and stage_name in ('Closed Won', 'Verbal Commitment')
    group by 1,2
),
cost_and_sales as (
    select
    b.month,
    b.source_type,
    a.cost,
    b.predicted_sales,
    b.predicted_sales/a.cost as predicted_roi
    from cost a
    full outer join sales b on (a.month = b.month and a.source_type = b.source_type)
)
select
*
from cost_and_sales
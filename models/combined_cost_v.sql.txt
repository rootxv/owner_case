{{
    config(
    materialized='view'
    )
}}

--need to convert the varchar columns to something more suitable
with formatted_advertising as (
    select
    TO_DATE(month, 'MON-YY') as month,
    --below removes all non-numeric characters and then replaces the comma with a period, then casts to a float
    --consider adjusting the input value during ingestion. data set only has USD for now but that might change in the future
    --could also use a jinja macro to convert this format if this is used often and ingestion can't be changed
    REPLACE(
        REGEXP_REPLACE(advertising, '[^0-9,]', ''),',', '.'
    )::FLOAT
    as advertising
    from {{ source('gtm_case', 'expenses_advertising') }}
),
--same thing but for the salaries and commisions
formatted_salary as (
    select
    TO_DATE(month, 'MON-YY') as month,
    REPLACE(
        REGEXP_REPLACE(outbound_sales_team, '[^0-9,]', ''),',', '.'
    )::FLOAT
    as outbound_sales_salary,
    REPLACE(
        REGEXP_REPLACE(inbound_sales_team, '[^0-9,]', ''),',', '.'
    )::FLOAT
    as inbound_sales_salary
    from {{ source('gtm_case', 'expenses_salary_and_commissions')}}
),
--combine the costs of both tables into one
combined_cost as (
    select
    a.month,
    a.advertising,
    b.inbound_sales_salary,
    advertising + inbound_sales_salary as total_inbound_cost, --total inbound cost is marketing+inbound sales team
    b.outbound_sales_salary --total outbound cost is just the outbound sales team
    from formatted_advertising a
    join formatted_salary b on (a.month = b.month)
)
select
*
from combined_cost
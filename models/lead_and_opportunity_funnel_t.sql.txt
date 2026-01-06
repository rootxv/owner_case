{{
    config(
    materialized='table'
    )
}}
--simplify, fix, and categorize some info from the leads table
with leads_info as (
    select
    lead_id,
    DATEADD(YEAR, 2000, form_submission_date) as form_submission_date, --years are missing 2000 vs actual
    sales_call_count+sales_text_count+sales_email_count as total_sales_contacts,
    LEAST(TO_DATE(first_sales_call_date),TO_DATE(first_text_sent_date)) as first_sales_activity_date, --getting earliest known sales activity. we don't know first email sent date
    TO_DATE(last_sales_activity_date) as last_sales_activity_date,
    TO_DATE(first_meeting_booked_date) as first_meeting_booked_date,
    REPLACE(predicted_sales_with_owner, ',', '.')::FLOAT as predicted_sales, --converts string to float
    case
    when (online_ordering_used in ('['''']', 'nan', '[]') or online_ordering_used is null)
        and (marketplaces_used in ('['''']', 'nan', '[]') or marketplaces_used is null)
    then 'no online used/unknown' else 'online used' end
    as online_status, --this gets whether or not we know they have any sort of online presence already
    cuisine_types,
    case
    when location_count = 1 then '1'
    when location_count between 2 and 10 then '2-10'
    when location_count between 11 and 25 then '11-25'
    when location_count between 26 and 100 then '26-100'
    when location_count > 100 then 'over 100'
    else 'unknown' end
    as location_count_bucket, --put business size into buckets for easier filtering, kind of arbitrary for now
    connected_with_decision_maker,
    status,
    converted_opportunity_id
    from {{ source('gtm_case', 'leads') }}
),
opportunities_info as (
    select
    opportunity_id,
    TO_DATE(created_date) as opportunity_created_date, --ts to date
    stage_name,
    lost_reason_c as lost_reason,
    closed_lost_notes_c as closed_lost_notes,
    business_issue_c as business_issue,
    how_did_you_hear_about_us_c as source,
    --want to consolidate the source types into inbound and outbound (and other) categories
    --this way we can compare outbound cost to outbound opportunities, inbound cost to inbound opportunities
    --keep original source above to maintain granularity for drilling down
    case
    when how_did_you_hear_about_us_c in ('Cold call', 'Webinar') then 'Outbound'
    when how_did_you_hear_about_us_c in ('Facebook/IG', 'Media Outlet', 'Youtube', 'Google') then 'Inbound'
    else 'organic/unknown'
    end as source_type,
    demo_held,
    DATEADD(YEAR, 2000, close_date) as close_date --year is behind by 2000 again
    from {{ source('gtm_case', 'opportunities') }}
),
combined_funnel as (
    select
    a.*,
    b.* exclude (opportunity_id) --remove this bc we already have opp id
    from leads_info a
    left join opportunities_info b on (a.converted_opportunity_id = b.opportunity_id)
)
select
*
from combined_funnel
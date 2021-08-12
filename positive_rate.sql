with base_arr as
(select
    week_start,
    region,
    country,
    seat_bucket,
    sum(arr_at_week_start) as arr_at_week_start
from
(select
    pa.entry_date::date as week_start,
    nvl(nullif(c.region,''),'Unknown') as region,
    nvl(nullif(c.country_code,''),'Unknown') as country,
    case when plan_size between 2 and 4 then '2 to 4'
        when plan_size between 5 and 10 then '5 to 10'
        when plan_size between 15 and 35 then '15 to 35'
        else '40 plus' end as seat_bucket,
    sum(real_plan_price)*12 as arr_at_week_start
from premium_accounts_by_day pa
left join domain_addons da
    on pa.customer_id = da.object_id
left join countries c
    on da.likely_geo = c.country_name
where pa.entry_date = %(snapshot_date)s
    and date_trunc('week',pa.entry_date) = pa.entry_date
    and real_plan_price > 0
group by 1,2,3,4)
group by 1,2,3,4),


delta_arr as
(select
    week_start,
    region,
    country,
    seat_bucket,
    sum(case when arr_grp = 'conversion' then arr end) as conversion_arr,
    sum(case when arr_grp like '%%expansion' then arr end) as expansion_arr,
    sum(case when arr_grp = 'churn' then arr end) as churn_arr,
    sum(case when arr_grp like '%%downgrade' then arr end) as downgrade_arr
from
(select
    date_trunc('week',customer_session_end)::date as week_start,
    nvl(nullif(c.region,''),'Unknown') as region,
    nvl(nullif(c.country_code,''),'Unknown') as country,
    case when nvl(nullif(prev_seat_count,0),final_seat_count) between 2 and 4 then '2 to 4'
        when nvl(nullif(prev_seat_count,0),final_seat_count) between 5 and 10 then '5 to 10'
        when nvl(nullif(prev_seat_count,0),final_seat_count) between 15 and 35 then '15 to 35'
        else '40 plus' end as seat_bucket,
    case
        when nvl(prev_real_price,0) <= 0
            and final_real_price > 0 then 'conversion'
        when nvl(final_real_price,0) <= 0
            and prev_real_price > 0 then 'churn'
        when prev_real_price > 0 and final_real_price > 0 and final_real_price > prev_real_price then case
            when ((prev_plan_tier = 'premium' and final_plan_tier in ('business','enterprise'))
                or (prev_plan_tier = 'business' and final_plan_tier = 'enterprise')) then 'tier'
            when seat_count_diff > 0 then 'seat'
            else 'other' end || ' ' || 'expansion'
        when prev_real_price > 0 and final_real_price > 0 and final_real_price < prev_real_price then case
            when ((final_plan_tier = 'premium' and prev_plan_tier in ('business','enterprise'))
                or (final_plan_tier = 'business' and prev_plan_tier = 'enterprise')) then 'tier'
            when seat_count_diff < 0 then 'seat'
            else 'other' end || ' ' || 'downgrade'
        else 'other'
    end as arr_grp,
    abs(sum(real_price_diff))*12 as arr
from premium_sessions_by_entity ps
left join domain_addons da
    on ps.customer_id = da.object_id
left join countries c
    on da.likely_geo = c.country_name
where date_trunc('week',customer_session_end)::date = %(snapshot_date)s
    and nvl(prev_real_price,0) != nvl(final_real_price,0)
group by 1,2,3,4,5)
group by 1,2,3,4)

select
    w.week_start,
    g.region,
    g.country,
    g.seat_bucket,
    nvl(base_arr.arr_at_week_start,0) as arr_at_week_start,
    nvl(delta_arr.conversion_arr,0) as conversion_arr,
    nvl(delta_arr.expansion_arr,0) as expansion_arr,
    nvl(delta_arr.churn_arr,0) as churn_arr,
    nvl(delta_arr.downgrade_arr,0) as downgrade_arr
from (select distinct week_start from base_arr) w
cross join (select distinct region, country, seat_bucket from base_arr) g
left join base_arr
    on w.week_start = base_arr.week_start
    and g.region = base_arr.region
    and g.country = base_arr.country
    and g.seat_bucket = base_arr.seat_bucket
left join delta_arr
    on w.week_start = delta_arr.week_start
    and g.region = delta_arr.region
    and g.country = delta_arr.country
    and g.seat_bucket = delta_arr.seat_bucket

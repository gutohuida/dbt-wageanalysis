{{
  config(
    materialized='view',
    tags=['lightdash']
  )
}}

select 
    iap.country,
    coalesce(cc.continent, 'Not found') as continent,
    iap.job, 
    iap.currency, 
    iap.pay, 
    iap.average_salary, 
    iap.pay_range_min, 
    iap.pay_range_max, 
    iap.base_pay_min, 
    iap.base_pay_max, 
    iap.additional_pay_min, 
    iap.additional_pay_max, 
    iap.base_period, 
    iap.last_update
from {{ref('int_adjusted_pay')}} iap
left join {{source('raw','country_continent')}} cc
on lower(iap.country) = lower(cc.name)
where currency_to = 'EUR'
{{
  config(
    materialized='view',
    tags=['lightdash']
  )
}}

select 
    country, 
    job, 
    currency, 
    pay, 
    average_salary, 
    pay_range_min, 
    pay_range_max, 
    base_pay_min, 
    base_pay_max, 
    additional_pay_min, 
    additional_pay_max, 
    base_period, 
    last_update
from {{ref('int_adjusted_pay')}}
where currency_to = 'BRL'
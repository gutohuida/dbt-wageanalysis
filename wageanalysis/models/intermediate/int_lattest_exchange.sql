{{
  config(
    materialized='view'
  )
}}

with lattest as (
select
	ce.currency_from as currency,
	ce.currency_to as currency_to,
	ce.exchange_rate,
	row_number() over (partition by currency_from, currency_to order by insert_date desc) as currency_rank
from {{source('raw', 'currency_exchange')}} ce )

select 
    * 
from lattest
where currency_rank = 1
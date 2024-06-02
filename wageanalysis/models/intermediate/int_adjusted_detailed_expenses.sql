{{
  config(
    materialized='view',
  )
}}

with currency_code as (
	select 
		sced.country,
		sced.city,
		sced.type,
		sced.amount,
		c.currency_code,
		sced.pay_min,
		sced.pay_max
	from {{ref('stg_country_expenses_detail')}} sced
	join {{source('raw', 'countrys')}} c
	on lower(sced.country) = lower(c."name")
	where sced.currency is not null
	)
	
select
	cc.country,
	case
        when cc.city is null then 'NationWide'
        else cc.city         
    end as city,
	cc.type,
	round(cc.amount / le.exchange_rate::numeric, 2) as amount,
	cc.currency_code as currency,
	le.currency_to,
	round(cc.pay_min / le.exchange_rate::numeric, 2) as pay_min,
	round(cc.pay_max / le.exchange_rate::numeric, 2) as pay_max
from currency_code cc
join {{ref('int_lattest_exchange')}} le
on cc.currency_code = le.currency
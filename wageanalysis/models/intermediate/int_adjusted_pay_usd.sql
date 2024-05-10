{{
  config(
    materialized='view',
    tags=['lightdash']
  )
}}

with lattest_country_job_info as (
select 
	country,
	job, 
	currency, 
	pay, 
	"period", 
	last_update, 
	average_salary,
	wage_description, 
	pay_range_min, 
	pay_range_max, 
	base_pay_min, 
	base_pay_max, 
	additional_pay_min, 
	additional_pay_max,
	row_number() over (partition by country, job order by last_update desc) as job_rank
from {{ref('stg_country_job_info')}} scji),

exchange_rate_rank as (
select
	ce.currency_from as currency,
	ce.exchange_rate,
	row_number() over (partition by currency_from order by insert_date desc) as currency_rank
from {{source('raw','currency_exchange')}} ce
where currency_to = 'USD'
),

lattest_exchange as (
select 
	*
from exchange_rate_rank
where currency_rank = 1
),

joined as (
select 
	lcji.country,
	lcji.job, 
	lcji.currency, 
	lcji.pay, 
	lcji."period", 
	lcji.last_update, 
	lcji.average_salary,
	lcji.wage_description, 
	lcji.pay_range_min, 
	lcji.pay_range_max, 
	lcji.base_pay_min, 
	lcji.base_pay_max, 
	lcji.additional_pay_min, 
	lcji.additional_pay_max,
	c."name",
	c.currency_name,
	c.currency_code,
	le.exchange_rate 
from lattest_country_job_info lcji
join {{source('raw','countrys')}} c 
on lower(lcji.country) = lower(c."name")
join lattest_exchange le 
on c.currency_code = le.currency 
where job_rank = 1
and concat_ws('-', lcji.country, lcji.currency, c.currency_code) 
	not in (select 
				concat_ws('-', country, currency, currency_code) 
			from {{ref('adt_currency_country')}} acc))
			
select
	country,
	job, 
	currency,
	case
		when "period" = 'mo' then round(((pay / exchange_rate) * 12)::numeric, 2)
		else round((pay / exchange_rate)::numeric, 2)
	end	as pay,
	case
		when "period" = 'mo' then round(((average_salary / exchange_rate) * 12)::numeric, 2)
		else round((average_salary / exchange_rate)::numeric, 2)
	end	as average_salary,
	case
		when "period" = 'mo' then round(((pay_range_min / exchange_rate) * 12)::numeric, 2)
		else round((pay_range_min / exchange_rate)::numeric, 2)
	end	as pay_range_min, 
	case
		when "period" = 'mo' then round(((pay_range_max / exchange_rate) * 12)::numeric, 2)
		else round((pay_range_max / exchange_rate)::numeric, 2)
	end	as pay_range_max, 
	case
		when "period" = 'mo' then round(((base_pay_min / exchange_rate) * 12)::numeric, 2)
		else round((base_pay_min / exchange_rate)::numeric, 2)
	end	as base_pay_min,
	case
		when "period" = 'mo' then round(((base_pay_max / exchange_rate) * 12)::numeric, 2)
		else round((base_pay_max / exchange_rate)::numeric, 2)
	end	as base_pay_max,
	case
		when "period" = 'mo' then round(((additional_pay_min / exchange_rate) * 12)::numeric, 2)
		else round((additional_pay_min / exchange_rate)::numeric, 2)
	end	as additional_pay_min,
	case
		when "period" = 'mo' then round(((additional_pay_max / exchange_rate) * 12)::numeric, 2)
		else round((additional_pay_max / exchange_rate)::numeric, 2)
	end	as additional_pay_max,
	"period" as base_period, 
	last_update
from joined
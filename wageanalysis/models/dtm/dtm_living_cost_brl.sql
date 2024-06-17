{{
  config(
    materialized='view',
    tags=['lightdash']
  )
}}

select
	iae.country,
	coalesce(cc.continent, 'Not found') as continent,
	iae.single + iade.amount as total_single_mo,
	(iae.single + iade.amount) * 12 as total_single_yr,
	iae.family_of_4 + iade.amount as total_family_mo,
	(iae.family_of_4 + iade.amount) * 12 as total_family_yr,
	iade.amount,
	iade."type" 
from {{ref('int_adjusted_expenses')}} iae 
join {{ref('int_adjusted_detailed_expenses')}} iade 
on iae.country = iade.country 
and iae.city = iade.city
and iae.currency_to = iade.currency_to
left join {{source('raw','country_continent')}} cc
on lower(iae.country) = lower(cc.name)
where 
	1=1 
and iade."type" like 'Apartment%'
and iae.currency_to = 'BRL'
and iae.city = 'NationWide'
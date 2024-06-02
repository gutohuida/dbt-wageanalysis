--{{ config(materialized='view') }}

with lattest as (
	select
		*,
		rank() over (partition by country, city order by insert_date desc) as rank
	from {{ source('raw','country_expenses')}} ce 
)

select 
	country,
	city,
	substring(substring(replace(family_of_4, ',', ''),'\d*\.\d*.'),'\d*\.\d*')::numeric as family_of_4,
	substring(substring(replace(single, ',', ''),'\d*\.\d*.'),'\d*\.\d*')::numeric as single,
	substring(replace(substring(replace(family_of_4, ',', ''),'\d*\.\d*.'), '.', ''),'\D') as currency
from lattest
where rank = 1
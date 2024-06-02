--{{ config(materialized='view') }}

with lattest as (
	select 
		*,
		rank() over (partition by country, city order by insert_date desc) as rank
	from  {{ source('raw','country_expenses_detail')}} ced 
)

select 
	country,
    city,
	"type",
	substring(replace(amount, ',', ''), '\d+.\d+')::numeric as amount,
	trim(substring(trim(replace(replace(amount, ',', ''), '.', '')), '\D+')) as currency,
	substring(replace(replace("range", chr(10),''),',',''), '^(\d+.\d+)')::numeric as pay_min,
	substring(replace(replace("range", chr(10),''),',',''), '(\d+.\d+)$')::numeric as pay_max
from lattest
where rank = 1
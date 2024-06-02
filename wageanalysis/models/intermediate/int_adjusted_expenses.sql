with corrected_currency as (
	select 
		sce.country,
		sce.city,
		sce.single,
		sce.family_of_4,
		case 
			when sce.currency = '€' then 'EUR'
			else 'XXX'
		end as currency	
	from {{ref('stg_country_expenses')}} sce)
	
select
	cc.country,
    case
        when cc.city is null then 'NationWide'
        else cc.city         
    end as city,
	round(cc.single / ile.exchange_rate::numeric, 2) as single,
	round(cc.family_of_4 / ile.exchange_rate::numeric, 2) as family_of_4,
	ile.currency_to 
from corrected_currency cc
join {{ref('int_lattest_exchange')}} ile 
on cc.currency = ile.currency
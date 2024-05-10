with lattest as (
    select 
        scji.country,
        row_number() over (partition by scji.country, scji.job order by scji.last_update desc) as rank
    from {{ref('stg_country_job_info')}} scji)

select l.country           
from lattest l
where lower(l.country::text) not in (select lower(c.name) from raw.countrys c )
and l.rank = 1
group by 1
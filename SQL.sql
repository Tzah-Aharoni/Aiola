select "CHLOCCT" , count(*) as num_of_flights
from "israel_flight_information"
where "CHAORD" = 'A' -- is on time or delayed column
group by "CHLOCCT"
order by count(*) desc
limit 3



select * 
from israel_flight_information ifi 
join country_info ci on ifi."CHLOCCT" = upper(ci."countryName")
where cast("areaInSqKm" as numeric) >= 1000000.0
  and "CHRMINE" = 'LANDED'

  
  
with cte as (
select distinct "countryName", count(*) over (partition by "CHLOCCT") as num_of_flights
from israel_flight_information ifi 
join country_info ci on ifi."CHLOCCT" = upper(ci."countryName")
where cast("population" as numeric) >= 10000000.0
)
select *, cte."num_of_flights" - lag(cte."num_of_flights") over (order by cte."countryName")
from cte 
where cte."num_of_flights" < 50
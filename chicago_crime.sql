SELECT  count(*) as crime_per_year, Extract(YEAR FROM date) as year
FROM `bigquery-public-data.chicago_crime.crime` 
group by year
order by year;
# top 5 crime types
SELECT primary_type, count(*) as count_types
from `bigquery-public-data.chicago_crime.crime`
group by primary_type
order by count_types desc
LIMIT 5;
# crimes per month
SELECT EXTRACT(MONTH FROM date) as month, count(*) as no_of_crimes
from `bigquery-public-data.chicago_crime.crime`
group by month
order by month;
# arrest rate by crime type
select primary_type,ROUND((100 *(AVG(CAST(arrest AS INT64)))),2) as arrest_rate 
FROM `bigquery-public-data.chicago_crime.crime`
group by primary_type;
# crimes by day of week
SELECT FORMAT_DATE('%A', DATE(date)) AS DAY_OF_WEEK, count(*) AS crimes
from `bigquery-public-data.chicago_crime.crime`
group by DAY_OF_WEEK
ORDER BY crimes DESC;
# rank crime types by total counts
SELECT primary_type,count(*) as cnt,RANK() over(order by count(*) desc) as rank
from `bigquery-public-data.chicago_crime.crime`
group by primary_type;
# top 3 crime types per year
with yearly_crime AS (
  SELECT EXTRACT(YEAR FROM date) as year, primary_type,count(*) as cnt
  from `bigquery-public-data.chicago_crime.crime`
  group by year, primary_type)
select *
from (select year, primary_type, cnt,
      DENSE_RANK() OVER(PARTITION BY year ORDER BY cnt DESC) AS rnk from yearly_crime)
where rnk<=3
order by rnk DESC;
#year over year crime change
with yearly as(
  select EXTRACT(YEAR FROM date) as year, count(*) as total_crimes
  from `bigquery-public-data.chicago_crime.crime`
  group by year
)
select year, total_crimes, total_crimes - LAG(total_crimes) OVER(order by year) as yoy_change
from yearly;
# rolling 3 years average of crimes
with yearly as(
  select EXTRACT(YEAR FROM date) as year, count(*) as total_crimes
  from `bigquery-public-data.chicago_crime.crime`
  group by year
)
select year, total_crimes, AVG(total_crimes) OVER(order by year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rol_avg
from yearly;
# other query for above
SELECT year, total_crimes,Avg(total_crimes) OVER(ORDER BY year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_avg
from(SELECT EXTRACT(YEAR FROM date)as year, count(*) as total_crimes
from `bigquery-public-data.chicago_crime.crime`
group by year);
#find years where crime increased compared to previous year
select * 
from(select year, total_crimes, LAG(total_crimes) OVER(ORDER BY year) as prev_year
from(
  select EXTRACT(YEAR FROM date) as year, count(*) as total_crimes
  from `bigquery-public-data.chicago_crime.crime`
  group by year))
  where total_crimes > prev_year;

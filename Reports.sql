
select to_date(b.date_time),country,region,count(distinct a.visitor_id) from  (
  select * from (
select *, row_number() over (partition by visitor_id,to_date(visit_start) order by visit_start desc ) rn
from singlife.sandbox.visitors_data --where visitor_id='37675618842.9285' 
  ) dt
where rn= 1
  ) a join (select date_time,visitor_id from searches_data group by date_time,visitor_id)b
on a.visitor_id=b.visitor_id
group by to_date(b.date_time),country,region order by 4 desc
create table report.dq_gi engine MergeTree order by qty_gi as
select toStartOfHour(toDateTime(dt)) dt_hour
     , uniq(gi_id) qty_gi
     , uniq(gi.wh_id) qty_wh
     , uniq(gi.office_id) qty_office
     , uniq(gi.employee_id) qty_empl
     , uniqIf(gi_id, status_id=3) qty_st_3
     , uniqIf(gi_id, status_id=2) qty_st_2
     , uniqIf(gi_id, status_id=1) qty_st_1
     , uniq(gi_id)/uniq(employee_id) gi_per_empl
     , uniq(gi_id)/uniq(wh_id) gi_per_wh
     , uniq(gi_id)/uniq(office_id) gi_per_office
from gi
group by dt_hour;

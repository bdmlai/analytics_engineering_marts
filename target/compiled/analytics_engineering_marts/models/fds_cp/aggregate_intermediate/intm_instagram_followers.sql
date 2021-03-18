
select
trunc(date_trunc('month',date)) as date,
dim_country_id,
sum(followers) as igm_followers
from
(select  date, dim_country_id,followers
from
(select dim_country_id,trunc(convert(datetime,convert(varchar(10),as_on_date))) as date,
sum(c_followers)as followers from "entdwdb"."fds_igm"."fact_ig_smfollowership_audience_bycountry" group by 1,2))
where followers is not null group by 1,2 order by 1,2 desc
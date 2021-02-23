delete from fds_yt.dim_channel
where channel_id in
(
select channel_id 
from
(
( 
SELECT 
CHANNEL_ID,
CHANNEL_NAME
FROM
(
SELECT 
CHANNEL_ID AS CHANNEL_ID,
TRIM(CHANNEL_NAME) AS CHANNEL_NAME,
ROW_NUMBER() OVER (PARTITION BY CHANNEL_ID ORDER BY REPORT_DATE, AS_ON_DATE,hour DESC) RN,
as_on_date,
hour
FROM 
fds_yt.youtube_video_metadata
where to_char(as_on_date,'yyyymmdd')  in ("+context.as_on_date+")
) DIM_CHANNEL
WHERE
RN = 1 
)
except
(
select channel_id,channel_name from fds_yt.dim_channel
)
)
);

insert into fds_yt.dim_channel
(
select channel_id,
channel_name,
getdate() as insert_timestamp,
"+context.batch_id+" as batch_id 
from
(
( 
SELECT 
CHANNEL_ID,
CHANNEL_NAME
FROM
(
SELECT 
CHANNEL_ID AS CHANNEL_ID,
TRIM(CHANNEL_NAME) AS CHANNEL_NAME,
ROW_NUMBER() OVER (PARTITION BY CHANNEL_ID ORDER BY REPORT_DATE, AS_ON_DATE,hour DESC) RN,
as_on_date,
hour
FROM 
fds_yt.youtube_video_metadata
where to_char(as_on_date,'yyyymmdd')  in ("+context.as_on_date+")

) DIM_CHANNEL
WHERE
RN = 1 
)
except
(
select channel_id,channel_name from fds_yt.dim_channel)
)
);
unload('select * from "+context.fds_yt_schema+"."+context.fds_tbl_name+"')
to  's3://"+context.fds_s3_bucket+"/"+context.fds_s3_key+context.fds_tbl_name+".csv'
iam_role '"+context.fds_s3_iam_role+"'
Header
ALLOWOVERWRITE
parallel off;
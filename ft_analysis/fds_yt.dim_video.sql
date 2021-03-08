delete from fds_yt.dim_video 
where exists (
select video_id,
video_vms_id,
video_wweid,
series_wweid
from
(
select 
VIDEO_ID,
video_vms_id,
video_wweid,
SERIES_WWEID,
TITLE,
time_uploaded,
--dateadd(m,-180,time_uploaded) as time_uploaded_est,
length from
( 
SELECT distinct
DIM_VIDEO.VIDEO_ID,
VMS_MDM.video_vms_id,
VMS_MDM.video_wweid,
VMS_MDM.SERIES_WWEID,
DIM_VIDEO.TITLE,
DIM_VIDEO.time_uploaded,
DIM_VIDEO.length
FROM
(
SELECT
VIDEO_ID,
TITLE,
time_uploaded,
length,
as_on_date
FROM
(
select 
video_id as video_id,
title as title,
time_uploaded,
length,
AS_ON_DATE,
ROW_NUMBER() over (partition by trim(video_id) order by REPORT_DATE, as_on_date,hour DESC) RN
from 
fds_yt.youtube_video_metadata
where ((to_char(as_on_date,'yyyymmdd') in ("+context.as_on_date+")) or (as_on_date in ("+context.as_on_date+")))
) IV_DIM_VIDEO
WHERE 
RN = 1
) DIM_VIDEO
LEFT OUTER JOIN
(
SELECT
VMS.VIDEO_YT_ID,
VMS.video_vms_id,
VMS.video_wweid,
EP.SERIES_WWEID
FROM
(
SELECT 
PRT.partner_ref as VIDEO_YT_ID,
PRT.VIDEO_ID AS VIDEO_VMS_ID,
vt.wweid as video_wweid,
row_number() over (partition by PRT.partner_ref,vt.video_id order by PRT.partner_ref,PRT.VIDEO_ID,vt.updated_at desc) RN
FROM
fds_vms.partner PRT
INNER JOIN fds_vms.video_tag VT 
ON
VT.VIDEO_ID = PRT.VIDEO_ID
WHERE
/*PRT.partner_id = 5 */ ((lower(PRT.partner_desc) like 'youtube%') or (lower(PRT.partner_desc) = 'youtube')) and vt.tag_name = 'CONTENT'
) VMS
LEFT OUTER JOIN fds_mdm.contents ep 
on
ep.content_wweid = VMS.video_wweid
where RN=1
) VMS_MDM
ON
DIM_VIDEO.VIDEO_ID = VMS_MDM.VIDEO_YT_ID
)
--except
--( select VIDEO_ID,video_vmsid,video_wweid,SERIES_WWEID,TITLE,time_uploaded,length from fds_yt.dim_video 
--)
)src
where src.video_id=fds_yt.dim_video.video_id
--and src.video_vms_id=fds_yt.dim_video.video_vmsid
--and src.video_wweid=fds_yt.dim_video.video_wweid
--and src.series_wweid=fds_yt.dim_video.series_wweid
);
insert into fds_yt.dim_video
(
select 
VIDEO_ID,
video_vms_id,
video_wweid,
SERIES_WWEID,
TITLE,
time_uploaded,
--dateadd(m,-180,time_uploaded) as time_uploaded_est,
length,
getdate() as insert_timestamp,
"+context.batch_id+" as batch_id 
from
(
select 
VIDEO_ID,
video_vms_id,
video_wweid,
SERIES_WWEID,
TITLE,
time_uploaded,
length
from
( 
SELECT distinct
DIM_VIDEO.VIDEO_ID,
VMS_MDM.video_vms_id,
VMS_MDM.video_wweid,
VMS_MDM.SERIES_WWEID,
DIM_VIDEO.TITLE,
DIM_VIDEO.time_uploaded,
DIM_VIDEO.length
FROM
(
SELECT
VIDEO_ID,
TITLE,
time_uploaded,
length,
as_on_date
FROM
(
select 
video_id as video_id,
title as title,
time_uploaded,
length,
AS_ON_DATE,
ROW_NUMBER() over (partition by trim(video_id) order by REPORT_DATE, as_on_date,hour DESC) RN
from 
fds_yt.youtube_video_metadata
where ((to_char(as_on_date,'yyyymmdd') in ("+context.as_on_date+")) or (as_on_date in ("+context.as_on_date+")))
) IV_DIM_VIDEO
WHERE 
RN = 1
) DIM_VIDEO
LEFT OUTER JOIN
(
SELECT
VMS.VIDEO_YT_ID,
VMS.video_vms_id,
VMS.video_wweid,
EP.SERIES_WWEID
FROM
(
SELECT 
PRT.partner_ref as VIDEO_YT_ID,
PRT.VIDEO_ID AS VIDEO_VMS_ID,
vt.wweid as video_wweid,
row_number() over (partition by PRT.partner_ref,vt.video_id order by PRT.partner_ref,PRT.VIDEO_ID,vt.updated_at desc) RN
FROM
fds_vms.partner PRT
INNER JOIN fds_vms.video_tag VT 
ON
VT.VIDEO_ID = PRT.VIDEO_ID
WHERE
/*PRT.partner_id = 5 */ ((lower(PRT.partner_desc) like 'youtube%') or (lower(PRT.partner_desc) = 'youtube')) and vt.tag_name = 'CONTENT' 
) VMS
LEFT OUTER JOIN fds_mdm.contents ep 
on
ep.content_wweid = VMS.video_wweid
where RN=1
) VMS_MDM
ON
DIM_VIDEO.VIDEO_ID = VMS_MDM.VIDEO_YT_ID
)
except
( select VIDEO_ID,video_vmsid,video_wweid,SERIES_WWEID,TITLE,time_uploaded,length from fds_yt.dim_video 
)));
unload('select * from fds_yt.dim_video')
to  's3://"+context.fds_s3_bucket+"/"+context.fds_s3_key+context.fds_tbl_name+".csv'
iam_role '"+context.fds_s3_iam_role+"'
delimiter '~'
Header
ALLOWOVERWRITE
parallel off;
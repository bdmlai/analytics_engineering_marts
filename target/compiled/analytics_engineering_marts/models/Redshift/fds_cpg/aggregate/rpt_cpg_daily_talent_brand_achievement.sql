
select distinct x.talent_description, x.entity_type, x.brand, c.achievement_name, 
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from 
	(select distinct i.src_talent_description as talent_description, a.entity_id, a.entity_type, b.brand
        from "entdwdb"."fds_cpg"."dim_cpg_item" i
        join "entdwdb"."fds_mdm"."alternateid" a on a.alternate_id_name = i.src_talent_id 
		    and a.entity_type in ('character_lineages') and a.alternate_id_type_name = 'Merch Sales'
        left join (select distinct character_lineage_id as entity_id, a.character_lineages_wweid, 
					character_lineage_name, designation as brand, start_date, end_date
					from "entdwdb"."fds_mdm"."character" a 
					left join "entdwdb"."fds_emm"."brand" b on a.character_lineages_wweid = b.character_lineage_wweid 
					where end_date is null) b on  a.entity_id = b.entity_id 
    union
     select distinct i.src_talent_description as talent_description, a.entity_id, a.entity_type, b.brand
        from "entdwdb"."fds_cpg"."dim_cpg_item" i
        join "entdwdb"."fds_mdm"."alternateid" a on a.alternate_id_name = i.src_talent_id 
		    and a.entity_type in ('group_lineages') and a.alternate_id_type_name = 'Merch Sales'
        left join
                (select * from 
                (select distinct group_lineage_id as entity_id, group_lineage_wweid, group_lineage_name, 
				  brand, start_date, end_date 
				  from (select *, row_number() over(partition by group_lineage_id order by start_date desc nulls last) as rank 
				  from (select a.*, designation as brand, start_date, end_date
                  from "entdwdb"."fds_mdm"."characterlineage_grouplineage" a 
				  left join "entdwdb"."fds_emm"."brand" b on a.character_lineage_wweid = b.character_lineage_wweid 
				  where end_date is null)) 
				  where rank = 1) 
				  where brand is not null) b on  a.entity_id = b.entity_id) x
left join 
        (select distinct b.achievement_name, b.lineage_type_name as achievement_type, 
		 b.lineage_name as achievement_lineage_name, c.characters_name as talent_name, c.characters_wweid, 
		 c.character_lineage_id as entity_id, 'character' as entity_type, a.start_date, a.end_date 
        from "entdwdb"."fds_mdm"."achievement_entity" a 
		left join "entdwdb"."fds_mdm"."achievement" b on a.achievement_id = b.id 
		left join "entdwdb"."fds_mdm"."character" c on a.entity_id = c.id
        where a.entity_type = 'characters' and b.lineage_type_name = 'Championship' and a.end_date is null 
        union
        select distinct b.achievement_name, b.lineage_type_name as achievement_type, 
		b.lineage_name as achievement_lineage_name, c.group_name, c.group_wweid, 
		c.character_lineage_mdmid as entity_id, 'group' as entity_type, a.start_date, a.end_date 
        from "entdwdb"."fds_mdm"."achievement_entity" a 
		left join "entdwdb"."fds_mdm"."achievement" b on a.achievement_id = b.id 
		left join "entdwdb"."fds_mdm"."groups" c on a.entity_id = c.mdmid
        where a.entity_type = 'groups' and b.lineage_type_name = 'Championship' and a.end_date is null) c on  x.entity_id = c.entity_id
where x.brand is not null
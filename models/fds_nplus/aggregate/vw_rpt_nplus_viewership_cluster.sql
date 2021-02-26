/***********************************************************************************/
/*                  9.  tier 3 model - final consolidated output -- View                   */
/* db:analytics_workspace schema:content                                           */
/* input tables: rpt_nplus_viewership_cluster_clustering               */
/* ouput tables: vw_nplus_viewership_cluster_clustering                          */
/***********************************************************************************/
{{
    config(
        materialized='view',
        tags=['viewership','viewership_model'],
		schema='fds_nplus'
		
    )
}}

select * from rpt_nplus_viewership_cluster_clustering; 
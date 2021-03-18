/***********************************************************************************/
/*                  8.  tier 3 model - final consolidated output                   */
/* db:analytics_workspace schema:content                                           */
/* input tables: intm_nplus_viewership_cluster_clustering_ly                       */
/*               intm_nplus_viewership_cluster_clustering_st                       */
/*               intm_nplus_viewership_cluster_clustering_mt                       */
/*               intm_nplus_viewership_cluster_clustering_lt                       */
/*               intm_nplus_viewership_cluster_clustering_all_time                 */
/* ouput tables: rpt_nplus_viewership_cluster_clustering                          */
/***********************************************************************************/
{{
  config({
    "schemas": 'fds_nplus',
	"materialized": 'view',
    "tags": ['viewership','viewership_model']
	})
}}


select * from {{ ref('rpt_nplus_viewership_cluster') }}
    



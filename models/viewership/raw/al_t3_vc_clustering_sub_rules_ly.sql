/***********************************************************************************/
/*                 TIER 3 MODEL - RULES BASED BEHAVIORS-Sub-Rules                  */
/* DB:ANALYTICS_WORKSPACE SCHEMA:CONTENT                                           */
/* INPUT TABLES: al_t3_vc_user_consumption_ly                                      */
/* OUPUT TABLES: al_t3_vc_clustering_sub_rules_ly                                  */
/***********************************************************************************/

--Business Rules for Rules Behaviors and Sampling Behaviors
-- SUB-BEHAVIORS

/* Rules for Big Events */
DROP TABLE IF EXISTS clustering_rules_1a;
CREATE temporary TABLE clustering_rules_1a AS(
SELECT *,
    (       CASE                                              ----Big Events Content Category Rules Behaviors---- 
         --Unclassified People Due to Less than one month activity
         WHEN act_months < 1 THEN 'Less than One Month Activity'

         --Low Event Viewership
         WHEN big_event_monthly < 60 or big_event_monthly IS NULL THEN 'Low Big Event Viewership'
        
                --'t1 completer, t2 completer, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and tier_2_pct_watched >= .7 and nxt_takeover_pct_watched >= .7 then 't1 completer, t2 completer, to completer'

        --'t1 completer, t2 completer, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 't1 completer, t2 completer, some to'

        --'t1 completer, t2 completer, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 't1 completer, t2 completer, sample to'

        --'t1 completer, t2 completer, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 't1 completer, t2 completer, weak to'

        --'t1 completer, some t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and nxt_takeover_pct_watched >= .7 then 't1 completer, some t2, to completer'

        --'t1 completer, some t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 't1 completer, some t2, some to'

        --'t1 completer, some t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 't1 completer, some t2, sample to'

        --'t1 completer, some t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 't1 completer, some t2, weak to'

        --'t1 completer, sample t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and nxt_takeover_pct_watched >= .7 then 't1 completer, sample t2, to completer'

        --'t1 completer, sample t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 't1 completer, sample t2, some to'

        --'t1 completer, sample t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 't1 completer, sample t2, sample to'

        --'t1 completer, sample t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 't1 completer, sample t2, weak to'

        --'t1 completer, weak t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and nxt_takeover_pct_watched >= .7 then 't1 completer, weak t2, to completer'

        --'t1 completer, weak t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 't1 completer, weak t2, some to'

        --'t1 completer, weak t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 't1 completer, weak t2, sample to'

        --'t1 completer, weak t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and tier_1_pct_watched >= .7 and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 't1 completer, weak t2, weak to'

        --'some t1, t2 completer, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and tier_2_pct_watched >= .7 and nxt_takeover_pct_watched >= .7 then 'some t1, t2 completer, to completer'

        --'some t1, t2 completer, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'some t1, t2 completer, some to'

        --'some t1, t2 completer, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'some t1, t2 completer, sample to'

        --'some t1, t2 completer, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'some t1, t2 completer, weak to'

        --'some t1, some t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and nxt_takeover_pct_watched >= .7 then 'some t1, some t2, to completer'

        --'some t1, some t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'some t1, some t2, some to'

        --'some t1, some t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'some t1, some t2, sample to'

        --'some t1, some t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'some t1, some t2, weak to'

        --'some t1, sample t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and nxt_takeover_pct_watched >= .7 then 'some t1, sample t2, to completer'

        --'some t1, sample t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'some t1, sample t2, some to'

        --'some t1, sample t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'some t1, sample t2, sample to'

        --'some t1, sample t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'some t1, sample t2, weak to'

        --'some t1, weak t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and nxt_takeover_pct_watched >= .7 then 'some t1, weak t2, to completer'

        --'some t1, weak t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'some t1, weak t2, some to'

        --'some t1, weak t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'some t1, weak t2, sample to'

        --'some t1, weak t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >= .3 and tier_1_pct_watched <.7) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'some t1, weak t2, weak to'

        --'sample t1, t2 completer, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and tier_2_pct_watched >= .7 and nxt_takeover_pct_watched >= .7 then 'sample t1, t2 completer, to completer'

        --'sample t1, t2 completer, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'sample t1, t2 completer, some to'

        --'sample t1, t2 completer, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'sample t1, t2 completer, sample to'

        --'sample t1, t2 completer, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'sample t1, t2 completer, weak to'

        --'sample t1, some t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and nxt_takeover_pct_watched >= .7 then 'sample t1, some t2, to completer'

        --'sample t1, some t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'sample t1, some t2, some to'

        --'sample t1, some t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'sample t1, some t2, sample to'

        --'sample t1, some t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'sample t1, some t2, weak to'

        --'sample t1, sample t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and nxt_takeover_pct_watched >= .7 then 'sample t1, sample t2, to completer'

        --'sample t1, sample t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'sample t1, sample t2, some to'

        --'sample t1, sample t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'sample t1, sample t2, sample to'

        --'sample t1, sample t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'sample t1, sample t2, weak to'

        --'sample t1, weak t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and nxt_takeover_pct_watched >= .7 then 'sample t1, weak t2, to completer'

        --'sample t1, weak t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'sample t1, weak t2, some to'

        --'sample t1, weak t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'sample t1, weak t2, sample to'

        --'sample t1, weak t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched >=.1 and tier_1_pct_watched <.3) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'sample t1, weak t2, weak to'

        --'weak t1, t2 completer, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and tier_2_pct_watched >= .7 and nxt_takeover_pct_watched >= .7 then 'weak t1, t2 completer, to completer'

        --'weak t1, t2 completer, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'weak t1, t2 completer, some to'

        --'weak t1, t2 completer, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'weak t1, t2 completer, sample to'

        --'weak t1, t2 completer, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and tier_2_pct_watched >= .7 and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'weak t1, t2 completer, weak to'

        --'weak t1, some t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and nxt_takeover_pct_watched >= .7 then 'weak t1, some t2, to completer'

        --'weak t1, some t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'weak t1, some t2, some to'

        --'weak t1, some t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'weak t1, some t2, sample to'

        --'weak t1, some t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched >= .3 and tier_2_pct_watched <.7) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'weak t1, some t2, weak to'

        --'weak t1, sample t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and nxt_takeover_pct_watched >= .7 then 'weak t1, sample t2, to completer'

        --'weak t1, sample t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'weak t1, sample t2, some to'

        --'weak t1, sample t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'weak t1, sample t2, sample to'

        --'weak t1, sample t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched >=.1 and tier_2_pct_watched <.3) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'weak t1, sample t2, weak to'

        --'weak t1, weak t2, to completer'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and nxt_takeover_pct_watched >= .7 then 'weak t1, weak t2, to completer'

        --'weak t1, weak t2, some to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched >=.3 and nxt_takeover_pct_watched <.7) then 'weak t1, weak t2, some to'

        --'weak t1, weak t2, sample to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched >=.1 and nxt_takeover_pct_watched < 3) then 'weak t1, weak t2, sample to'

        --'weak t1, weak t2, weak to'
        WHEN big_event_monthly >=60 and act_months >=1 and (tier_1_pct_watched < .1 or tier_1_pct_watched IS NULL) and (tier_2_pct_watched < .1 or tier_2_pct_watched IS NULL) and (nxt_takeover_pct_watched < .1 or nxt_takeover_pct_watched IS NULL) then 'weak t1, weak t2, weak to'
       --Everything Else will get sent through the Clustering Algo in Python
       ELSE 'For Big Event Classification' END) AS big_event_classification

FROM al_t3_vc_user_consumption_ly); 


DROP TABLE IF EXISTS clustering_rules_1b;
CREATE TEMPORARY TABLE clustering_rules_1b AS(
SELECT *,  
       case when act_months < 1 THEN 'Less than One Month Activity'
            when big_event_monthly < 60 or big_event_monthly IS NULL THEN 'Low Big Event Viewership'

            -- low consumption on all BE shows
            -- TO < 30% and TO < 10%
            when (tier_1_pct_watched_clean < 0.3 or tier_1_pct_watched_clean is null)
                    and (tier_2_pct_watched_clean < 0.3 or tier_2_pct_watched_clean is null)
                    and (nxt_takeover_pct_watched < 0.1 or nxt_takeover_pct_watched is null)
                 then 'PPV Sampler'   
            
            -- t1/t2 < 30%, TO between 10% and 30%
            when (tier_1_pct_watched_clean < 0.3 or tier_1_pct_watched_clean is null)
                    and (tier_2_pct_watched_clean < 0.3 or tier_2_pct_watched_clean is null)
                    and (nxt_takeover_pct_watched < 0.3 or nxt_takeover_pct_watched is null)
                 then 'Big Event Sampler'     
         
            /* watch one show only */
             --  t1 >= 30%, t2 < 30%, TO < 10%
            when tier_1_pct_watched_clean >= 0.3 and tier_2_pct_watched_clean < 0.3 and (nxt_takeover_pct_watched < 0.1 or nxt_takeover_pct_watched is null)
                 then 'Tier 1 PPV Only'
             --  t1 < 30%, t2 >= 30%, TO < 10%
            when tier_1_pct_watched_clean < 0.3 and tier_2_pct_watched_clean >= 0.3 and (nxt_takeover_pct_watched < 0.1 or nxt_takeover_pct_watched is null)
                 then 'Tier 2 PPV Only'

            /* t1/t2/to completer */
              -- all above 70%
            when tier_1_pct_watched_clean >= 0.7 and tier_2_pct_watched_clean >= 0.7 and nxt_takeover_pct_watched >= 0.7
                 then 'Big Event Die-Hard'
            
            /* t1/t1 completer but only some TO */
             -- t1 & t2 >= 70%, TO <= 10%
            when (tier_1_pct_watched_clean >= 0.7 and tier_2_pct_watched_clean >= 0.7 and (nxt_takeover_pct_watched < 0.1 or nxt_takeover_pct_watched is null))
                 then 'PPV Die-Hard'
            
            /* t1/t1 completer but only some TO */
             -- t1 & t2 < 70%, TO >= 30%
            when tier_1_pct_watched_clean < 0.3 and tier_2_pct_watched_clean < 0.3 and (nxt_takeover_pct_watched >= 0.3)
                 then 'TakeOver Preference'
             
             -- relatively strong consumption on at least one of T1 PPV or T2 PPV, TO below 31%
             -- (T1 >=30% or T2 >= 30%), TO < 10%
            when (tier_1_pct_watched_clean >= 0.3 and tier_2_pct_watched_clean >= 0.3 and (nxt_takeover_pct_watched < 0.1 or nxt_takeover_pct_watched is null))
              or (tier_1_pct_watched_clean >= 0.3 and (tier_2_pct_watched_clean < 0.3 or tier_2_pct_watched_clean is null) and (nxt_takeover_pct_watched < 0.1 or nxt_takeover_pct_watched is null))
              or ((tier_1_pct_watched_clean < 0.3 or tier_1_pct_watched_clean is null) and tier_2_pct_watched_clean >= 0.3 and (nxt_takeover_pct_watched < 0.1 or nxt_takeover_pct_watched is null)) 
                 then 'PPV Aficionado'
         
             -- completing PPVs but not TOs
            when (tier_1_pct_watched_clean >= 0.7 and tier_2_pct_watched_clean >= 0.7 and nxt_takeover_pct_watched >= 0.1 and nxt_takeover_pct_watched < 0.7)
              then 'PPV Completer'

             -- relatively strong consumption on TO and at least one of T1 PPV or T2 PPV
             -- (T1 >=30% or T2 >= 30%), TO >= 10%, but not completing everything  
            when (tier_1_pct_watched_clean >= 0.3 and tier_2_pct_watched_clean >= 0.3 and nxt_takeover_pct_watched >= 0.1)
              or (tier_1_pct_watched_clean >= 0.3 and (tier_2_pct_watched_clean < 0.3 or tier_2_pct_watched_clean is null) and nxt_takeover_pct_watched >= 0.1)
              or ((tier_1_pct_watched_clean < 0.3 or tier_1_pct_watched_clean is null) and tier_2_pct_watched_clean >= 0.3 and nxt_takeover_pct_watched >= 0.1)
              then 'Big Event Aficionado'

            else 'Other_2' end as big_event_classification_v2       
from clustering_rules_1a);

/* Rules for Network In-Ring */
-- enhancement: is this part still necesary?
update clustering_rules_1b set nxt_pct_watched = 0 where nxt_pct_watched is null;
update clustering_rules_1b set _205_live_pct_watched = 0 where _205_live_pct_watched is null;
update clustering_rules_1b set nxt_uk_pct_watched = 0 where nxt_uk_pct_watched is null;
update clustering_rules_1b set in_ring_tournament_pct_watched = 0 where in_ring_tournament_pct_watched is null;
update clustering_rules_1b set in_ring_special_pct_watched = 0 where in_ring_special_pct_watched is null;



/* Rules for Network In-Ring -- in-rin gspecial v2*/
DROP TABLE IF EXISTS clustering_rules_2;
CREATE TEMPORARY TABLE clustering_rules_2 AS
select *,
       case when act_months is null or time_window is null then null
            when act_months < 1 then 'Less than One Month Activity'
            when network_in_ring_monthly < 60 then 'Low Network In-Ring Viewership'

            when (nxt_pct_watched <= 0.3 or nxt_pct_watched is null) 
                  and (_205_live_pct_watched <= 0.3 or _205_live_pct_watched is null)
                  and (nxt_uk_pct_watched <= 0.3 or nxt_uk_pct_watched is null) 
                  and (in_ring_tournament_pct_watched <= 0.3 or in_ring_tournament_pct_watched is null)
                  and (in_ring_special_pct_watched <= 0.3 or in_ring_special_pct_watched is null)
              then 'In-Ring Sampler'

            when (nxt_pct_watched > 0.3 and _205_live_pct_watched > 0.3 and nxt_uk_pct_watched > 0.3 and in_ring_tournament_pct_watched  > 0.3 and in_ring_special_pct_watched > 0.3)
              or (_205_live_pct_watched > 0.3 and nxt_uk_pct_watched > 0.3 and in_ring_tournament_pct_watched  > 0.3 and in_ring_special_pct_watched > 0.3)
              or (nxt_pct_watched > 0.3 and nxt_uk_pct_watched > 0.3 and in_ring_tournament_pct_watched  > 0.3 and in_ring_special_pct_watched > 0.3)
              or (nxt_pct_watched > 0.3 and _205_live_pct_watched > 0.3 and in_ring_tournament_pct_watched  > 0.3 and in_ring_special_pct_watched > 0.3)
              or (nxt_pct_watched > 0.3 and _205_live_pct_watched > 0.3 and nxt_uk_pct_watched > 0.3 and in_ring_special_pct_watched > 0.3)
              or (nxt_pct_watched > 0.3 and _205_live_pct_watched > 0.3 and nxt_uk_pct_watched > 0.3 and in_ring_tournament_pct_watched  > 0.3 )
            then 'In-Ring Die-Hard'

            when nxt_pct_watched > nxt_uk_pct_watched
             and nxt_pct_watched > _205_live_pct_watched
             and nxt_pct_watched > in_ring_tournament_pct_watched
             and nxt_pct_watched > in_ring_special_pct_watched
             then 'NXT Preference'
             
            when nxt_uk_pct_watched > nxt_pct_watched
                 and nxt_uk_pct_watched > _205_live_pct_watched
                 and nxt_uk_pct_watched > in_ring_tournament_pct_watched
                 and nxt_uk_pct_watched > in_ring_special_pct_watched
                 then 'NXT UK Preference'
                 
            when _205_live_pct_watched > nxt_pct_watched
                 and _205_live_pct_watched > nxt_uk_pct_watched
                 and _205_live_pct_watched > in_ring_tournament_pct_watched
                 and _205_live_pct_watched > in_ring_special_pct_watched
                 then '205 Live Preference'
                 
            when in_ring_tournament_pct_watched > nxt_pct_watched
                 and in_ring_tournament_pct_watched > nxt_uk_pct_watched
                 and in_ring_tournament_pct_watched > _205_live_pct_watched
                 and in_ring_tournament_pct_watched > in_ring_special_pct_watched
                 then 'In-Ring Tournament Preference'

            when in_ring_special_pct_watched > nxt_pct_watched
                 and in_ring_special_pct_watched > nxt_uk_pct_watched
                 and in_ring_special_pct_watched > _205_live_pct_watched
                 and in_ring_special_pct_watched > in_ring_tournament_pct_watched
                 then 'In-Ring Special Preference'
          else 'other' end as network_in_ring_classification
    from clustering_rules_1b;

/* Rules for Originals */
DROP TABLE IF EXISTS clustering_rules_3;
CREATE TEMPORARY TABLE clustering_rules_3 AS
SELECT *,
       CASE                                                
         --Unclassified People Due to Less than one month activity
         WHEN act_months < 1 THEN 'Less than One Month Activity'
         
         --Low Original Viewership
         WHEN original_monthly < 60 or original_monthly IS NULL THEN 'Low Original Viewership'
         
         -- sampler
            when historic_original/act_months < 30 and network_historic_original/act_months < 30 and network_new_original/act_months < 30
                 then 'Original Sampler'
            
            -- relatively strong engagement on only one category
              -- historic original preference
            when historic_original/act_months >= 30 
                   and network_historic_original/act_months < 30
                   and network_new_original/act_months < 30
                  then 'Non-Network Historic Original Preference'
            
               -- network historic original preference
            when historic_original/act_months < 30 
                   and network_historic_original/act_months >= 30
                   and network_new_original/act_months < 30
                  then 'Network Historic Original Preference'    

               -- network new original preference
            when historic_original/act_months < 30 
                   and network_historic_original/act_months < 30
                   and network_new_original/act_months >= 30
                  then 'Network New Original Preference'        

            -- relatively strong engagement on two categories
               -- non-network historic and network historic preference
            when historic_original/act_months >= 30 
                   and network_historic_original/act_months >= 30
                   and network_new_original/act_months < 30
                  then 'Non-Network Historic and Network Historic Original Preference'
         
               -- non-network historic and network new original preference
            when historic_original/act_months >= 30 
                   and network_historic_original/act_months < 30
                   and network_new_original/act_months >= 30
                  then 'Non-Network Historic and Network New Original Preference'
               
               -- network historic and network new preference   
            when historic_original/act_months < 30 
                   and network_historic_original/act_months >= 30
                   and network_new_original/act_months >= 30
                  then 'Network Historic and Network New Original Preference'    

            -- relatively strong engagement on all three categories
            when historic_original/act_months >= 30 
                   and network_historic_original/act_months >= 30
                   and network_new_original/act_months >= 30
                  then 'Original Die-Hard'   
            
            -- QC -- should be no 'other' group
            else 'Other' END AS original_classification
from clustering_rules_2;



--- HERE


/* Rules for Historic In-Ring */
-- calculates monthly consumption
drop table IF EXISTS historic;
CREATE TEMPORARY TABLE historic as
select src_fan_id, act_months,
       historic_in_ring,
       historic_in_ring_monthly,
       attitude_era,
       ecw,
       golden_era,
       historic_network_in_ring,
       indie_wrestling,
       new_era,
       reality_era,
       ruthless_aggression,
       the_new_generation_era,
       wcw,
       attitude_era/act_months as attitude_era_monthly,
       ecw/act_months as ecw_monthly,
       golden_era/act_months as golden_era_monthly,
       historic_network_in_ring/act_months as historic_network_in_ring_monthly,
       indie_wrestling/act_months as indie_wrestling_monthly,
       new_era/act_months as new_era_monthly,
       reality_era/act_months as reality_era_monthly,
       ruthless_aggression/act_months as ruthless_aggression_monthly,
       the_new_generation_era/act_months as the_new_generation_era_monthly,
       wcw/act_months as wcw_monthly
 from clustering_rules_3
 where historic_in_ring_monthly >= 60
 and historic_in_ring_monthly is not null
 and act_months is not null
 and time_window is not null;


-- create flags for each historic in-ring category
DROP TABLE IF EXISTS hist_flag;
CREATE TEMPORARY TABLE hist_flag AS
SELECT src_fan_id,
       attitude_era,
       ecw,
       golden_era,
       historic_network_in_ring,
       indie_wrestling,
       new_era,
       reality_era,
       ruthless_aggression,
       the_new_generation_era,
       wcw,
       case when attitude_era_monthly >= 30 then 1 else 0 end as attitude_era_f,
       case when ecw_monthly >= 30 then 1 else 0 end as ecw_f,
       case when golden_era_monthly >= 30 then 1 else 0 end as golden_era_f,
       case when historic_network_in_ring_monthly >= 30 then 1 else 0 end as historic_network_in_ring_f,
       case when indie_wrestling_monthly >= 30 then 1 else 0 end as indie_wrestling_f,
       case when new_era_monthly >= 30 then 1 else 0 end as new_era_f,
       case when reality_era_monthly >= 30 then 1 else 0 end as reality_era_f,
       case when ruthless_aggression_monthly >= 30 then 1 else 0 end as ruthless_aggression_f,
       case when the_new_generation_era_monthly >= 30 then 1 else 0 end as the_new_generation_era_f,
       case when wcw_monthly >= 30 then 1 else 0 end as wcw_f
  from historic;

DROP TABLE IF EXISTS hist_variety_score;
CREATE TEMPORARY TABLE hist_variety_score as
SELECT *,
       attitude_era_f + ecw_f + golden_era_f + historic_network_in_ring_f + indie_wrestling_f +
       new_era_f + reality_era_f + ruthless_aggression_f + the_new_generation_era_f +wcw_f as variety_score
  from hist_flag;

drop table IF EXISTS hist_behav;
CREATE TEMPORARY TABLE hist_behav as
select src_fan_id,
case when variety_score = 0 then 'Historic In-Ring Sampler' 
     when variety_score > 4 then 'Historic In-Ring Die-Hard'
     
     when attitude_era >= ecw
           and attitude_era >= golden_era
           and attitude_era >= historic_network_in_ring
           and attitude_era >= indie_wrestling
           and attitude_era >= new_era
           and attitude_era >= reality_era
           and attitude_era >= ruthless_aggression
           and attitude_era >= the_new_generation_era
           and attitude_era >= wcw
           then 'Attitude Era Preference'
           
      when ecw >= attitude_era
           and ecw >= golden_era
           and ecw >= historic_network_in_ring
           and ecw >= indie_wrestling
           and ecw >= new_era
           and ecw >= reality_era
           and ecw >= ruthless_aggression
           and ecw >= the_new_generation_era
           and ecw >= wcw
           then 'ECW Preference'

      when golden_era >= attitude_era
           and golden_era >= ecw
           and golden_era >= historic_network_in_ring
           and golden_era >= indie_wrestling
           and golden_era >= new_era
           and golden_era >= reality_era
           and golden_era >= ruthless_aggression
           and golden_era >= the_new_generation_era
           and golden_era >= wcw
           then 'Golden Era Preference'

      when historic_network_in_ring >= attitude_era
           and historic_network_in_ring >= ecw
           and historic_network_in_ring >= golden_era
           and historic_network_in_ring >= indie_wrestling
           and historic_network_in_ring >= new_era
           and historic_network_in_ring >= reality_era
           and historic_network_in_ring >= ruthless_aggression
           and historic_network_in_ring >= the_new_generation_era
           and historic_network_in_ring >= wcw
           then 'Historic Network In-Ring Preference'

      when indie_wrestling >= attitude_era
           and indie_wrestling >= ecw
           and indie_wrestling >= golden_era
           and indie_wrestling >= historic_network_in_ring
           and indie_wrestling >= new_era
           and indie_wrestling >= reality_era
           and indie_wrestling >= ruthless_aggression
           and indie_wrestling >= the_new_generation_era
           and indie_wrestling >= wcw
           then 'Indie Wrestling Preference'

      when new_era >= attitude_era
           and new_era >= ecw
           and new_era >= golden_era
           and new_era >= historic_network_in_ring
           and new_era >= indie_wrestling
           and new_era >= reality_era
           and new_era >= ruthless_aggression
           and new_era >= the_new_generation_era
           and new_era >= wcw
           then 'New Era Preference'

      when reality_era >= attitude_era
           and reality_era >= ecw
           and reality_era >= golden_era
           and reality_era >= historic_network_in_ring
           and reality_era >= indie_wrestling
           and reality_era >= new_era
           and reality_era >= ruthless_aggression
           and reality_era >= the_new_generation_era
           and reality_era >= wcw
           then 'Reality Era Preference'

      when ruthless_aggression >= attitude_era
           and ruthless_aggression >= ecw
           and ruthless_aggression >= golden_era
           and ruthless_aggression >= historic_network_in_ring
           and ruthless_aggression >= indie_wrestling
           and ruthless_aggression >= new_era
           and ruthless_aggression >= reality_era
           and ruthless_aggression >= the_new_generation_era
           and ruthless_aggression >= wcw
           then 'Ruthless Aggression Era Preference'

      when the_new_generation_era >= attitude_era
           and the_new_generation_era >= ecw
           and the_new_generation_era >= golden_era
           and the_new_generation_era >= historic_network_in_ring
           and the_new_generation_era >= indie_wrestling
           and the_new_generation_era >= new_era
           and the_new_generation_era >= reality_era
           and the_new_generation_era >= ruthless_aggression
           and the_new_generation_era >= wcw
           then 'New Generation Era Preference'

      when wcw >= attitude_era
           and wcw >= ecw
           and wcw >= golden_era
           and wcw >= historic_network_in_ring
           and wcw >= indie_wrestling
           and wcw >= new_era
           and wcw >= reality_era
           and wcw >= ruthless_aggression
           and wcw >= the_new_generation_era
           then 'WCW Preference'

        else 'other' end as historic_in_ring_classification  --QC: should be no other group
 from hist_variety_score;


DROP TABLE IF EXISTS clustering_rules_4;
CREATE TEMPORARY TABLE clustering_rules_4 AS
SELECT t1.*, t2.historic_in_ring_classification
  from clustering_rules_3 AS t1
  LEFT Join hist_behav as t2
  on t1.src_fan_id = t2.src_fan_id;

-- update low viewership behavior
UPDATE clustering_rules_4  
   SET historic_in_ring_classification = 'Low Historic In-Ring Viewership'
 WHERE historic_in_ring_monthly < 60 or historic_in_ring_monthly IS NULL;  

-- update new signups
UPDATE clustering_rules_4  
   SET historic_in_ring_classification = 'Less than One Month Activity'
 WHERE act_months < 1;   


drop table if exists al_t3_vc_clustering_sub_rules_ly;
create table al_t3_vc_clustering_sub_rules_ly as select * from clustering_rules_4;
grant select on al_t3_vc_clustering_sub_rules_ly to public;





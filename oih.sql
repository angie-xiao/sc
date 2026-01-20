/*
/*+ETLM {
    depend:{
        replace:[
            {name:"andes.oih_ddl.OIH_RECOMMEND_DETAIL"}, partition: {type: "Marketplaces", values: [7]}
            {name:"andes.roi_ml_ddl.vendor_company_codes"},partition: {type: "Marketplaces", values: [7]}
            {name:"andes.polo.pma_contacts"},partition: {type: "Marketplaces", values: [7]}
            {name:"andes.gip_fcst_ddl.O_ASIN_WEEKLY_FORECASTS_V2"},partition: {type: "Marketplaces", values: [7]}
        ]
    }
}*/
*/

/****************************************************
            get latest available date
****************************************************/
DROP TABLE IF EXISTS latest_date;
CREATE TEMP TABLE latest_date AS (
    select max(rundate::date) as rundate
    from andes.oih_ddl.OIH_RECOMMEND_DETAIL  
    where region_id=1
        and realm='CAAmazon'
        and gl in ( 199, 75, 194, 325, 121, 510, 364 )  
);

/****************************************************
                     OIH report 
****************************************************/
-- report on the last available Sat as of query run date 
DROP TABLE IF EXISTS oih_report_calcs;
CREATE TEMP TABLE oih_report_calcs AS (
    
    with cte1 as (
        select 
            d.rundate::date as rundate,
            gl, vendor, asin, title,
            exclusion_reason_code,
            buyer_exclusion_reason,
            sum(oih_action_quantity) as total_oih_quantity, 
            max(vendor_cost_on_receipt) as vendor_cost_on_receipt
        from andes.oih_ddl.OIH_RECOMMEND_DETAIL d
        where region_id=1
            and realm='CAAmazon'
            and gl in ( 199, 75, 194, 325, 121, 510, 364)                               -- Consumables
            and d.rundate::date = (select rundate from latest_date)                     -- latest available date filter    
            -- and rundate::date = '2026-01-03'::date                                   -- manual date filter
        group by
            d.rundate::date, gl, vendor, asin, title,
            exclusion_reason_code, buyer_exclusion_reason
    )

    select 
        cte1.*, 
        (cte1.total_oih_quantity * cte1.vendor_cost_on_receipt) as total_oih_cost
    from cte1
    where total_oih_quantity > 0 
);


DROP TABLE IF EXISTS oih_report_calcs_comp;
CREATE TEMP TABLE oih_report_calcs_comp AS (
    select 
        oih.rundate,
        oih.gl, vcc.company_code,
        oih.asin, oih.title,
        oih.exclusion_reason_code,
        oih.buyer_exclusion_reason,
        sum(oih.total_oih_quantity) as total_oih_quantity,
        sum(oih.vendor_cost_on_receipt) as vendor_cost_on_receipt,
        sum(oih.total_oih_cost) as total_oih_cost
    from oih_report_calcs oih
        left join andes.roi_ml_ddl.vendor_company_codes vcc
        on oih.vendor = vcc.vendor_code
    group by
        oih.rundate,
        oih.gl, vcc.company_code,
        oih.asin, oih.title,
        oih.exclusion_reason_code,
        oih.buyer_exclusion_reason
);



DROP TABLE IF EXISTS auto_noofer;
CREATE TEMP TABLE auto_noofer AS (
    select *
    from oih_report_calcs_comp
    where upper(exclusion_reason_code) = 'AUTO_NOOFFER'
);

DROP TABLE IF EXISTS other_types_excl;
CREATE TEMP TABLE other_types_excl AS (
    select *
    from oih_report_calcs_comp
    where upper(exclusion_reason_code) != 'AUTO_NOOFFER'
);


-- "Top 10 Overall OIH ASINs"
DROP TABLE IF EXISTS oih_overall;
CREATE TEMP TABLE oih_overall AS (
    with cte as (
        select * from auto_noofer
        UNION ALL
        select * from other_types_excl
    ),
    rk as (
        select *,  dense_rank() over(order by total_oih_cost desc) as rk
        from cte
    )
    select 
        rk.rundate, rk.gl, rk.company_code, rk.asin, rk.title,
        rk.exclusion_reason_code,
        rk.total_oih_quantity, rk.total_oih_cost,
        'oih_overall' as table_name
    from rk 
    where rk <= 10  
);
 
-- "Top 10 No offer OIH ASINs"
DROP TABLE IF EXISTS oih_no_offer;
CREATE TEMP TABLE oih_no_offer AS (
    with rk as (
        select *, dense_rank() over(order by total_oih_cost desc) as rk
        from auto_noofer
    )       
    select 
        rk.rundate, rk.gl, rk.company_code, rk.asin, rk.title,
        rk.exclusion_reason_code,
        rk.total_oih_quantity, rk.total_oih_cost,
        'oih_no_offer' as table_name
    from rk 
    where rk <= 10          
);

-- putting OIH reports together
DROP TABLE IF EXISTS oih_report;
CREATE TEMP TABLE oih_report AS (
    SELECT * FROM oih_overall
    UNION ALL
    SELECT * FROM oih_no_offer
);


/****************************************************
                 mapping owner
****************************************************/
-- https://datacentral.a2z.com/hoot/providers/af356408-2477-4af4-8c63-f8fd32216c5f/tables/pma_contacts/versions/9?tab=schema
DROP TABLE IF EXISTS oih_vm;
CREATE TEMP TABLE oih_vm AS (
    WITH pma_vm AS (
        SELECT company_code, gl, user_id
        FROM andes.polo.pma_contacts
        WHERE role = 'VendorManager'
          AND region = 'NA'
          AND marketscope = 'CA'
          AND deleted = 0
          AND invalid = 0
    )
    SELECT
        oih.*,
        pma_vm.user_id AS vm_alias
    FROM oih_report oih
    LEFT JOIN pma_vm
      ON pma_vm.company_code = oih.company_code
     AND pma_vm.gl = oih.gl
);


/****************************************************
        forecast (buyable asins only)
****************************************************/
--https://datacentral.a2z.com/providers/gip-fcst/tables/O_ASIN_WEEKLY_FORECASTS_V2/versions/5?tab=schema
DROP TABLE IF EXISTS forecast;
CREATE TEMPORARY TABLE forecast AS (

    with forecast_date as 
    (
        SELECT MAX(forecast_creation_day) as forecast_creation_day FROM ANDES.GIP_FCST_DDL.O_ASIN_WEEKLY_FORECASTS_V2 WHERE region_id = 1 AND marketplace_id = 7
    ),

    weekly_forecast as (
        SELECT 
            fd.forecast_creation_day
            , f.marketplace_id
            , f.asin
            , f.gl_product_group
            , f.model_id
            , f.forecast_type_code
            , COALESCE(forecast_quantity_week_0, 0) as forecast_quantity_week_0
            , COALESCE(forecast_quantity_week_1, 0) as forecast_quantity_week_1
            , COALESCE(forecast_quantity_week_2, 0) as forecast_quantity_week_2
            , COALESCE(forecast_quantity_week_3, 0) as forecast_quantity_week_3
            , COALESCE(forecast_quantity_week_4, 0) as forecast_quantity_week_4
        FROM ANDES.GIP_FCST_DDL.O_ASIN_WEEKLY_FORECASTS_V2 f
            INNER JOIN forecast_date fd
                ON f.forecast_creation_day = fd.forecast_creation_day
            INNER JOIN oih_report o
                ON f.asin = o.asin
        where f.region_id=1
            and f.marketplace_id=7
            and f.FORECAST_TYPE_CODE = 'P70'
    )

    SELECT *, 
        (forecast_quantity_week_0+forecast_quantity_week_1+forecast_quantity_week_2+forecast_quantity_week_3+forecast_quantity_week_4)
        /5 as p70_weekly_avg
    from weekly_forecast
);


/****************************************************
                WoC (buyable asins)
****************************************************/
DROP TABLE IF EXISTS woc_buyable;
CREATE TEMPORARY TABLE woc_buyable AS (
    SELECT o.*,
           f.p70_weekly_avg,
           CASE
               WHEN f.p70_weekly_avg IS NULL OR f.p70_weekly_avg = 0 THEN 0
               ELSE o.total_oih_quantity / f.p70_weekly_avg
           END as oih_woc
    FROM oih_vm o
        LEFT JOIN forecast f
        ON o.asin = f.asin
);


SELECT * FROM woc_buyable;

/****************************************************
        asin buyability (Selection Central)
****************************************************/
-- https://datacentral.a2z.com/cradle#/CAISM_Hubble_Admin/profiles/506da073-b39c-4d10-89ef-1aad711ceb97
-- DROP TABLE IF EXISTS buyability;
-- CREATE TEMP TABLE buyability AS (
--     select * 
--     from "andes_ext"."scp-cia"."catalog_contribution_issue_events" 
--     where marketplace_id=7
--         and sku='B0BGT84G2R'
        
-- );

 

 
-- /****************************************************
--        Denali supply (last hour of daily snapshot)
-- ****************************************************/
-- DROP TABLE IF EXISTS denali_supply;
-- CREATE TEMP TABLE denali_supply AS (
--     select snapshot_day, max(snapshot_hour) as snapshot_hour, fnsku, supply_type, sum(quantity) as qty
--     from andes.fa_gpi_inventory_analytics.gpi_inventory_supply
--     where region_id = 1
--         and owner_id = 11 -- ca
--         and snapshot_day::date in (select distinct rundate from oih_focus)
--         and fnsku in (select asin from oih_focus)
--     group by snapshot_day, fnsku, supply_type
-- );


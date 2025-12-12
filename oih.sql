
/****************************************************
                     OIH report 
****************************************************/
-- weekly on sats
DROP TABLE IF EXISTS oih_report_calcs;
CREATE TEMP TABLE oih_report_calcs AS (
    with cte1 as (
        select 
            rundate::date as rundate,
            gl, vendor, asin, title,
            exclusion_reason_code,
            buyer_exclusion_reason,
            sum(oih_action_quantity) as total_oih_quantity, 
            max(vendor_cost_on_receipt) as vendor_cost_on_receipt
        from andes.oih_ddl.OIH_RECOMMEND_DETAIL  
        where region_id=1
            and realm='CAAmazon'
            and gl in ( 199, 75, 194, 325, 121, 510, 364 ) -- Consumables
            and rundate::date = '2025-11-22'::date         -- test
        group by
            rundate::date,
            gl, vendor, asin, title,
            exclusion_reason_code, buyer_exclusion_reason
    )

    select 
        cte1.*, 
        (cte1.total_oih_quantity * cte1.vendor_cost_on_receipt) as total_oih_cost
    from cte1
    where total_oih_quantity > 0 
);

DROP TABLE IF EXISTS auto_noofer;
CREATE TEMP TABLE auto_noofer AS (
    select *
    from oih_report_calcs
    where upper(exclusion_reason_code) = 'AUTO_NOOFFER'
);

DROP TABLE IF EXISTS other_types_excl;
CREATE TEMP TABLE other_types_excl AS (
    select *
    from oih_report_calcs
    where upper(exclusion_reason_code) != 'AUTO_NOOFFER'
);


-- first table in Juan's report
DROP TABLE IF EXISTS oih_focus;
CREATE TEMP TABLE oih_focus AS (
    with cte as (
        select * from auto_noofer
        union ALL
        select * from other_types_excl
    ),
    rk as (
        select *,  dense_rank() over(order by total_oih_cost desc) as rk
        from cte
    )
    select 
        rk.rundate, rk.gl, c.company_code, rk.vendor, rk.asin, rk.title,
        rk.exclusion_reason_code,
        rk.total_oih_quantity, rk.total_oih_cost
    from rk 
        left join andes.roi_ml_ddl.vendor_company_codes c
        on rk.vendor = c.vendor_code
    where rk <= 10  
);
 

/****************************************************
                 mapping owner
****************************************************/
-- polo contacts
-- https://datacentral.a2z.com/hoot/providers/af356408-2477-4af4-8c63-f8fd32216c5f/tables/pma_contacts/versions/9?tab=schema
DROP TABLE IF EXISTS oih_vm;
CREATE TEMP TABLE oih_vm AS (
    SELECT
        oih.*,
        pma.user_id as vm_alias
    FROM oih_focus oih
        LEFT JOIN andes.polo.pma_contacts pma
            ON pma.company_code = oih.company_code
            AND oih.gl = pma.gl
    WHERE pma.role = 'VendorManager'
            AND pma.region = 'NA'
            AND pma.marketscope = 'CA'
            and pma.deleted=0
);


/****************************************************
        forecast (buyable asins only?)
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
            INNER JOIN other_types_excl o
            on f.asin = o.asin
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
    SELECT o.*, f.p70_weekly_avg, COALESCE(o.total_oih_quantity/f.p70_weekly_avg, 0) as oih_woc 
    FROM oih_vm o
        LEFT JOIN forecast f
        ON o.asin = f.asin 
);


/****************************************************
        asin buyability (Selection Central)
****************************************************/
DROP TABLE IF EXISTS buyability;
CREATE TEMP TABLE buyability AS (
    select * 
    from "andes_ext"."scp-cia"."catalog_contribution_issue_events" 
    where marketplace_id=7
        and sku='B0BGT84G2R'
        
);

 

 
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


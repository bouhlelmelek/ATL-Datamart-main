-- Création de l'extension dblink si elle n'existe pas déjà
CREATE EXTENSION IF NOT EXISTS dblink;
-- Insertion dans DimensionTemporelle
INSERT INTO public.DimensionTemporelle (pickup_datetime, dropoff_datetime)
SELECT DISTINCT t.tpep_pickup_datetime::timestamp without time zone,
    t.tpep_dropoff_datetime::timestamp without time zone
FROM dblink(
        'dbname=nyc_warehouse user=postgres password=admin',
        'SELECT tpep_pickup_datetime, tpep_dropoff_datetime FROM public.nyc_raw WHERE tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL LIMIT 500'
    ) AS t(
        tpep_pickup_datetime timestamp without time zone,
        tpep_dropoff_datetime timestamp without time zone
    );
-- Insertion dans DimensionLieu
INSERT INTO public.DimensionLieu (pulocationid, dolocationid)
SELECT DISTINCT t.pulocationid::integer,
    t.dolocationid::integer
FROM dblink(
        'dbname=nyc_warehouse user=postgres password=admin',
        'SELECT pulocationid, dolocationid FROM public.nyc_raw WHERE pulocationid IS NOT NULL AND dolocationid IS NOT NULL LIMIT 500'
    ) AS t(pulocationid integer, dolocationid integer);
-- Insertion dans DimensionFournisseur
INSERT INTO public.DimensionFournisseur (vendorid)
SELECT DISTINCT t.vendorid::integer
FROM dblink(
        'dbname=nyc_warehouse user=postgres password=admin',
        'SELECT vendorid FROM public.nyc_raw WHERE vendorid IS NOT NULL LIMIT 500'
    ) AS t(vendorid integer);
-- Insertion dans DimensionTarification
INSERT INTO public.DimensionTarification (ratecodeid)
SELECT DISTINCT t.ratecodeid::integer
FROM dblink(
        'dbname=nyc_warehouse user=postgres password=admin',
        'SELECT ratecodeid FROM public.nyc_raw WHERE ratecodeid IS NOT NULL LIMIT 500'
    ) AS t(ratecodeid integer);
-- Insertion dans DimensionPaiement
INSERT INTO public.DimensionPaiement (payment_type)
SELECT DISTINCT t.payment_type::integer
FROM dblink(
        'dbname=nyc_warehouse user=postgres password=admin',
        'SELECT payment_type FROM public.nyc_raw WHERE payment_type IS NOT NULL LIMIT 500'
    ) AS t(payment_type integer);
-- ...
-- Insertion dans FactTable
INSERT INTO public.FactTable (
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        vendorid,
        ratecodeid,
        payment_type,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee
    )
SELECT t.tpep_pickup_datetime::timestamp without time zone,
    t.tpep_dropoff_datetime::timestamp without time zone,
    t.pulocationid::integer,
    t.dolocationid::integer,
    t.vendorid::integer,
    t.ratecodeid::integer,
    t.payment_type::integer,
    t.trip_distance::numeric,
    t.fare_amount::numeric,
    t.extra::numeric,
    t.mta_tax::numeric,
    t.tip_amount::numeric,
    t.tolls_amount::numeric,
    t.improvement_surcharge::numeric,
    t.total_amount::numeric,
    t.congestion_surcharge::numeric,
    t.airport_fee::numeric
FROM dblink(
        'dbname=nyc_warehouse user=postgres password=admin',
        'SELECT 
        tpep_pickup_datetime, 
        tpep_dropoff_datetime, 
        pulocationid, 
        dolocationid, 
        vendorid, 
        ratecodeid, 
        payment_type, 
        trip_distance, 
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount, 
        tolls_amount, 
        improvement_surcharge, 
        total_amount, 
        congestion_surcharge, 
        airport_fee 
    FROM public.nyc_raw 
    WHERE 
        tpep_pickup_datetime IS NOT NULL AND 
        tpep_dropoff_datetime IS NOT NULL AND 
        pulocationid IS NOT NULL AND 
        dolocationid IS NOT NULL AND 
        vendorid IS NOT NULL AND 
        ratecodeid IS NOT NULL AND 
        payment_type IS NOT NULL AND 
        trip_distance IS NOT NULL AND 
        fare_amount IS NOT NULL AND 
        extra IS NOT NULL AND 
        mta_tax IS NOT NULL AND 
        tip_amount IS NOT NULL AND 
        tolls_amount IS NOT NULL AND 
        improvement_surcharge IS NOT NULL AND 
        total_amount IS NOT NULL AND 
        congestion_surcharge IS NOT NULL AND 
        airport_fee IS NOT NULL LIMIT 500'
    ) AS t(
        tpep_pickup_datetime timestamp without time zone,
        tpep_dropoff_datetime timestamp without time zone,
        pulocationid integer,
        dolocationid integer,
        vendorid integer,
        ratecodeid integer,
        payment_type integer,
        trip_distance numeric,
        fare_amount numeric,
        extra numeric,
        mta_tax numeric,
        tip_amount numeric,
        tolls_amount numeric,
        improvement_surcharge numeric,
        total_amount numeric,
        congestion_surcharge numeric,
        airport_fee numeric
    ) ON CONFLICT (
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        vendorid,
        ratecodeid,
        payment_type
    ) DO NOTHING;
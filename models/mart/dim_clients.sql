{{
  config(
    materialized = "table"
  )
}}

with
    mysql_clients as (select * from {{ ref('stg_mysql_client') }}),
    
    postgres_clients_patients_rel as (select distinct client_id from {{ ref('clients_patients_rel') }}),

    postgres_clients as 
    (
        select 
        c.*, CASE WHEN cp.client_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_client
        from {{ source("postgres", "clients") }} as c 
        left join postgres_clients_patients_rel cp 
        on c.id = cp.client_id
    ),

    postal_addresses as (
        select
            id,
            region,
            locality,
            postal_code,
            country_code,
            street_address_1,
            street_address_2
        from {{ source("postgres", "postal_addresses") }}
    )
-- 1. Matching records
    select
       TO_HEX(MD5((pc.email))) as unique_key,
        pc.id as client_id,
        mc.id as bolt_id,
        pc.email,
        pc.phone,
        --pc.created_at,
        mc.created as created_at,
        CAST(updated_at AS DATETIME) as updated_at,  -- Force TIMESTAMP
        CAST(disabled_at AS DATETIME) as disabled_at,
        family_name,
        pa.street_address_1,
        pa.street_address_2,
        pa.locality,
        pa.region,
        pa.postal_code,
        pa.country_code,
        'postgres' as source,
        CASE WHEN mc.is_client = TRUE OR pc.is_client = TRUE THEN TRUE ELSE FALSE END AS is_client

    from 
        postgres_clients pc 
    left join mysql_clients mc on pc.email=mc.email
    left join postal_addresses pa on pc.postal_address_id=pa.id
    where mc.id is not null

union all
-- 2. MySQL-only records
    select
        TO_HEX(MD5((mc.email))) as unique_key,
        null as client_id,
        mc.id as bold_id,
        mc.email,
        mc.phone,
        mc.created as created_at,
        cast(null as DATETIME) as updated_at,
        cast(null as DATETIME) as disabled_at,
        last_name as family_name,
        address1 as street_address_1,
        address2 as street_address_2,
        city as locality,
        state as region,
        zip as postal_code,
        'US' as country_code,
        'mysql' as source,
        mc.is_client as is_client

    from
        mysql_clients mc 
    where mc.email not in (select distinct email from postgres_clients)

    union all

-- 3. PostgreSQL-only records (FIX)
select
    TO_HEX(MD5(pc.email)) as unique_key,
    pc.id as client_id,
    null as bolt_id,
    pc.email,
    pc.phone,
    CAST(created_at AS DATETIME) as created_at,  -- Force TIMESTAMP
    CAST(updated_at AS DATETIME) as updated_at,  -- Force TIMESTAMP
    CAST(disabled_at AS DATETIME) as disabled_at,
    pc.family_name,
    pa.street_address_1,
    pa.street_address_2,
    pa.locality,
    pa.region,
    pa.postal_code,
    pa.country_code,
    'postgres' as source,
    pc.is_client as is_client

from
    postgres_clients pc
left join postal_addresses pa on pc.postal_address_id = pa.id
left join mysql_clients mc on pc.email = mc.email
where mc.email is null
{{
  config(
    materialized = "table"
  )
}}

with
    mysql_patients as (select * from {{ ref('stg_mysql_patients') }}),

    postgres_patients as (select * from {{ source("postgres", "patients") }}),

    ownership as (select * from {{ source("postgres", "ownerships") }}),

    breed as (select * from {{ source("postgres", "patient_breeds") }}),

    sex as (select * from {{ source("bolt_mysql", "sex") }}),

    species as (select * from {{ source("bolt_mysql", "species") }}),

    breeds as (select * from {{ source("bolt_mysql", "breeds") }})

    select
        dc.unique_key,
        dc.client_id,
        pp.id as patient_id,
        pp.color,
        pp.notes,
        pp.gender,
        pp.species,
        pp.created_at,
        pp.given_name,
        pp.updated_at,
        pp.family_name,
        pp.is_neutered,
        pp.microchip_id,
        pp.date_of_birth,
        pp.date_of_death,
        b.name as breed_name,
        sb.name as secondary_breed_name,
        'postgres' as source

    from 
        {{ ref('dim_clients') }} dc 
    left join ownership o on dc.client_id=o.client_id
    left join postgres_patients pp on o.patient_id=pp.id
    left join breed b on pp.primary_breed_id=b.id
    left join breed sb on pp.primary_breed_id=sb.id
    where dc.source='postgres' and pp.id is not null

union all

    select
        dc.unique_key,
        dc.client_id,
        null as patient_id,
        mp.color,
        null as notes,
        s.name as gender,
        sp.species_name as species,
        cast(mp.date_created as timestamp) as created_at,
        mp.name as given_name,
        cast(null as timestamp) as updated_at,
        dc.family_name,
        null as is_neutered,
        cast(mp.microchip_id as STRING), 
        mp.birthdate,
        mp.death_date,
        bs.breed_name as breed_name,
        sbs.breed_name as secondary_breed_name,
        'mysql' as source
    from 
        {{ ref('dim_clients') }} dc 
    left join mysql_patients mp on dc.email=mp.email
    left join sex s on mp.sex_id=s.sex_id
    left join species sp on mp.species_id=cast(sp.species_id as int)
    left join breeds bs on mp.primary_breed_id=cast(bs.breed_id as int)
    left join breeds sbs on mp.primary_breed_id=cast(sbs.breed_id as int)
    where dc.source='mysql' and mp.email is not null

{{
  config(
    materialized = "table"
  )
}}

With cte_is_patients AS 
(
    select distinct patient_id from {{ ref('stg_mysql_clients_patients_rel') }}
)
SELECT 
    p.*, 
    c.email,
    CASE WHEN rel.patient_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_patient
FROM {{ source("bolt_mysql", "patients") }} p
LEFT JOIN {{ source("bolt_mysql", "clients") }} c
    ON SAFE_CAST(p.client_id AS INT64) = SAFE_CAST(c.client_id AS INT64)
LEFT JOIN cte_is_patients rel
    ON p.patient_id = rel.patient_id
where c.email <> ''
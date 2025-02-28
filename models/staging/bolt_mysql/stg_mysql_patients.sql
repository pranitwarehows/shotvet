{{
  config(
    materialized = "table"
  )
}}

SELECT 
    p.*, 
    c.email 
FROM {{ source("bolt_mysql", "patients") }} p
LEFT JOIN {{ source("bolt_mysql", "clients") }} c 
ON SAFE_CAST(p.client_id AS INT64) = SAFE_CAST(c.client_id AS INT64)
where c.email <> ''
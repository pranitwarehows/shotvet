{{
  config(
    materialized = "table"
  )
}}

SELECT
DISTINCT i.patient_id,p.client_id
FROM {{ source("bolt_mysql", "invoice_items") }} AS i
INNER JOIN {{ source("bolt_mysql", "patients") }} AS p 
ON i.patient_id = p.patient_id
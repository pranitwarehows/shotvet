{{
  config(
    materialized = "table"
  )
}}

SELECT 
    distinct
    c.id as client_id, 
    p.patient_id as patient_id
FROM 
    {{ source("postgres", "work_orders") }} wo 
LEFT JOIN {{ source("postgres", "patient_registrations") }} p ON wo.patient_registration_id = p.id
LEFT JOIN {{ source("postgres", "patients") }} pat ON p.patient_id = pat.id
LEFT JOIN {{ source("postgres", "client_registrations") }} cr ON p.client_registration_id = cr.id
LEFT JOIN {{ source("postgres", "clients") }} c ON cr.client_id = c.id
{{
  config(
    materialized = "table"
  )
}}

SELECT

wo.id as work_order_id,
c.email as email,
c.client_id as lovepet_client_id, 
p.patient_id as lovepet_patient_id,
CONCAT(pat.given_name, ' ', pat.family_name) as pet_name,
wo.created_at as invoice_date,
wo.status as work_order_status,
lw.type,
lw.description,
wol.created_at as vaccine_given_date,
wol.follow_up_after as vaccine_due_date,
pf.created_at as product_given_date,
pf.follow_up_after as product_due_date,
tr.created_at as test_given_date

FROM {{ source("postgres", "work_orders") }} wo
LEFT JOIN {{ source("postgres", "work_order_line_items") }} lw ON wo.id = lw.work_order_id
LEFT JOIN {{ source("postgres", "vaccinations") }} wol ON lw.id = wol.work_order_line_item_id
LEFT JOIN {{ source("postgres", "product_fulfillments") }} pf ON lw.id = pf.work_order_line_item_id
LEFT JOIN {{ source("postgres", "test_results") }} tr ON lw.id = tr.work_order_line_item_id
LEFT JOIN {{ source("postgres", "patient_registrations") }} p ON wo.patient_registration_id = p.id
LEFT JOIN {{ ref('dim_patients') }} pat ON p.patient_id = pat.patient_id
LEFT JOIN {{ source("postgres", "client_registrations") }} cr ON p.client_registration_id = cr.id
LEFT JOIN {{ ref('dim_clients') }} c ON cr.client_id = c.client_id

WHERE wo.status = 'completed' 
and concat(pat.given_name,' ',pat.family_name) is not null

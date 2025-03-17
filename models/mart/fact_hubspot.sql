{{
  config(
    materialized = "table"
  )
}}

SELECT 
p.patient_id as lovepet_patient_id,
concat(pat.given_name,' ',pat.family_name) as pet_name,
max(case when lw.description like 'Rabies%' and wol.created_at is not null then wol.created_at else null end ) as  rabies_given,
max(case when lw.description like 'Rabies%' and pf.follow_up_after is not null then pf.follow_up_after else null end ) as  rabies_due,
max(case when lw.description like 'DA2PP%' and wol.created_at is not null then wol.created_at else null  end ) as  da2pp_given,
max(case when lw.description like 'DA2PP%' and pf.follow_up_after is not null then pf.follow_up_after else null end ) as  da2pp_due,
max(case when lw.description like 'Bordetella%' and wol.created_at is not null then wol.created_at else null end ) as  bord_given,
max(case when lw.description like 'Bordetella%' and pf.follow_up_after is not null then pf.follow_up_after else null end ) as  bord_due,
max(case when lw.description like 'Influenza%' and wol.created_at is not null then wol.created_at else null end ) as  flu_given,
max(case when lw.description like 'Influenza%' and pf.follow_up_after is not null then pf.follow_up_after else null end ) as  influenza_due,
max(case when lw.description like 'FVRCP%' and wol.created_at is not null then wol.created_at else null end ) as  fvrcp_given,
max(case when lw.description like 'FVRCP%' and pf.follow_up_after is not null then pf.follow_up_after else null end ) as  fvrcp_due,
max(case when lw.description like 'Lepto%' and wol.created_at is not null then wol.created_at else null end ) as  lepto_given,
max(case when lw.description like 'Lepto%' and pf.follow_up_after is not null then pf.follow_up_after else null end ) as  leptospirosis_due,
max(case when lw.description like 'FELV%' and wol.created_at is not null then wol.created_at else null end ) as  felv_given,
max(case when lw.description like 'FELV%' and pf.follow_up_after is not null then pf.follow_up_after else null end ) as  felv_due,
max(case when lw.description like 'Lyme%' and wol.created_at is not null then wol.created_at else null end ) as  lyme_given,
max(case when lw.description like 'Lyme%' and pf.follow_up_after is not null then pf.follow_up_after else null end ) as  lyme_due,
max(case when (lw.description like 'Trio%' OR lw.description like 'Tri-Heart Plus%' OR lw.description like 'Heartgard Plus%'
OR lw.description like 'Interceptor Plus%' OR lw.description like 'Trifexis%' OR lw.description like 'Sentinel Spectrum%' 
OR lw.description like 'Sentinel%'  OR lw.description like 'NexGard Plus%') and wol.created_at is not null 
then wol.created_at else null end ) as  hwp_given,
max(case when (lw.description like 'Trio%' OR lw.description like 'Tri-Heart Plus%' OR lw.description like 'Heartgard Plus%'
OR lw.description like 'Interceptor Plus%' OR lw.description like 'Trifexis%' OR lw.description like 'Sentinel Spectrum%' 
OR lw.description like 'Sentinel%'  OR lw.description like 'NexGard Plus%') and pf.follow_up_after is not null 
then pf.follow_up_after  else null end ) as  heartworm_prevention_due,
max(case when (lw.description like 'Revolution Plus Feline%' OR lw.description like 'Bravecto Feline Plus%' OR lw.description like 'Revolution Feline%') and wol.created_at is not null 
then wol.created_at else null end ) as  feline_bravecto_given,
max(case when (lw.description like 'Revolution Plus Feline%' OR lw.description like 'Bravecto Feline Plus%' OR lw.description like 'Revolution Feline%') and pf.follow_up_after is not null 
then pf.follow_up_after  else null end ) as  feline_bravecto_due,
max(case when (lw.description like 'Bravecto Canine%' OR lw.description like 'Simparica%' OR lw.description like 'Nexgard%' OR lw.description like 'Advantage Multi for Cats%') and wol.created_at is not null 
then wol.created_at else null end ) as  flea_tick_given,
max(case when (lw.description like 'Bravecto Canine%' OR lw.description like 'Simparica%' OR lw.description like 'Nexgard%' OR lw.description like 'Advantage Multi for Cats%') and pf.follow_up_after is not null 
then pf.follow_up_after  else null end ) as  flea___tick_due,
max(case when lw.description like 'Heartworm Test%' and wol.created_at is not null then wol.created_at else null end ) as  hwt_given,
max(case when lw.description like 'Heartworm Test%' and pf.follow_up_after is not null then pf.follow_up_after else null end ) as  heartworm_due
FROM {{ source("postgres", "work_orders") }} wo
LEFT JOIN {{ source("postgres", "work_order_line_items") }} lw ON wo.id = lw.work_order_id
LEFT JOIN {{ source("postgres", "vaccinations") }} wol ON lw.id = wol.work_order_line_item_id
LEFT JOIN {{ source("postgres", "product_fulfillments") }} pf ON lw.id = pf.work_order_line_item_id
LEFT JOIN {{ source("postgres", "test_results") }} tr ON lw.id = tr.work_order_line_item_id
LEFT JOIN {{ source("postgres", "patient_registrations") }} p ON wo.patient_registration_id = p.id
LEFT JOIN {{ ref('dim_patients') }} pat ON p.patient_id = pat.patient_id
LEFT JOIN{{ source("postgres", "client_registrations") }} cr ON p.client_registration_id = cr.id
LEFT JOIN {{ ref('dim_clients') }} c ON cr.client_id = c.client_id
WHERE wo.status = 'completed' 
and concat(pat.given_name,' ',pat.family_name) is not null
GROUP BY ALL 
ORDER BY 1
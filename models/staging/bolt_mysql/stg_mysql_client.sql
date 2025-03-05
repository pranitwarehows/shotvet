{{ 
    config(materialized="table") 
}}

with cte as 
(
    select *, row_number() over (partition by email order by created desc) rn
    from {{ source("bolt_mysql", "clients") }}
),
cte_is_client AS 
(
    select distinct client_id from {{ ref('stg_mysql_clients_patients_rel') }}
)
SELECT 
    cte._airbyte_raw_id,
    cte._airbyte_extracted_at,
    cte._airbyte_meta,
    cte._airbyte_generation_id,
    cte.id,
    cte.zip,
    cte.city,
    cte.email,
    cte.phone,
    cte.state,
    cte.balance,
    cte.bounced,
    cte.country,
    cte.created,
    cte.deleted,
    cte.address1,
    cte.address2,
    cte.inactive,
    cte.client_id,
    cte.last_name,
    cte.opted_out,
    cte.alert_text,
    cte.first_name,
    cte.unsubscribe,
    cte.woo_user_id,
    cte.location_code,
    cte.bigcommerce_id,
    cte.crm_account_id,
    cte.crm_contact_id,
    cte.merged_into_id,
    CASE WHEN cte_is_client.client_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_client
FROM cte
LEFT JOIN cte_is_client 
    ON SAFE_CAST(cte.client_id AS INT) = cte_is_client.client_id
WHERE rn = 1 AND email <> ''
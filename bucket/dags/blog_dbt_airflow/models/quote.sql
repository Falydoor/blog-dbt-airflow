{{
    config(
        materialized='incremental'
    )
}}

SELECT *
FROM public.quote

{% if is_incremental() %}
  WHERE inserted_at > (SELECT max(inserted_at) FROM {{ this }})
{% endif %}
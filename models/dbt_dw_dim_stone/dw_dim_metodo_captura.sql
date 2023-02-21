{{
  config(
    materialized='incremental',
    unique_key='metodo_captura_id',
    incremental_strategy='delete+insert',
    post_hook="{{ set_last_execution() }}; {{ dbt_model_grant() }}",
    dist='metodo_captura_id',
    sort= 'metodo_captura_id'
  )  
}}

with 
	{% if is_incremental() %}
dim_metodo_captura
as
(
 select metodo_captura_id from {{this}}
),
    {% endif %}	
metodo_captura
as
(
select distinct
    upper(metodo_captura) as metodo_captura
    ,case 
        when metodo_captura = 'pos' then 'POS'
        when metodo_captura = 'tap' then 'Tap Ton'
        when metodo_captura = 'link' then 'Link de Pagamento'
     else 'Indefinido' end as metodo_captura_desc
    ,md5(metodo_captura) as metodo_captura_id
    ,getdate() as source_updated_dt    
from dbt_dw_stone.stg_stone
)
select 
    a.metodo_captura_id
    ,a.metodo_captura
    ,a.metodo_captura_desc
    ,a.source_updated_dt
from metodo_captura a
	{% if is_incremental() %}
    left join dim_metodo_captura b on a.metodo_captura_id = b.metodo_captura_id
    where 
        b.metodo_captura_id is null
    {% endif %}	   
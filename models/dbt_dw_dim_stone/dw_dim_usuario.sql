{{
  config(
    materialized='incremental',
    unique_key='cod_usuario_id',
    incremental_strategy='delete+insert',
    post_hook="{{ set_last_execution() }}; {{ dbt_model_grant() }}",
    dist='cod_usuario_id',
    sort= 'cod_usuario_id'
  )  
}}


with 
	{% if is_incremental() %}
dim_usuario
as
(
 select cod_usuario_id from {{this}}
),
    {% endif %}	
 atualiza_usuario
as
(
select
    md5(a.codigo_usuario) cod_usuario_id
    ,a.codigo_usuario
    ,a.estado_usuario
    ,a.cidade_usuario
from dbt_dw_stone.stg_stone a
)
select a.* from atualiza_usuario a 
	{% if is_incremental() %}
    left join dim_usuario b on a.cod_usuario_id = b.cod_usuario_id
    where 
        b.cod_usuario_id   is null
    {% endif %}	
{{
  config(
    materialized='incremental',
    unique_key='info_cartao_pag_id',
    incremental_strategy='delete+insert',
    post_hook="{{ set_last_execution() }}; {{ dbt_model_grant() }}",
    dist='info_cartao_pag_id',
    sort= 'info_cartao_pag_id'
  )  
}} 
 
with 
	{% if is_incremental() %}
dim_info_cartao_pag
as
(
 select info_cartao_pag_id from {{this}}
),
    {% endif %}	
info_cartao_pag
as
(
select distinct 
    bandeira_cartao
    ,upper(metodo_pagamento) as metodo_pagamento
    ,upper(estado_transacao) as estado_transacao
    ,md5(bandeira_cartao+metodo_pagamento+estado_transacao) as info_cartao_pag_id
    ,getdate() as source_updated_dt    
from dbt_dw_stone.stg_stone
)
select
    a.info_cartao_pag_id
    ,bandeira_cartao
    ,a.metodo_pagamento
    ,a.estado_transacao
    ,a.source_updated_dt
from info_cartao_pag a
	{% if is_incremental() %}
    left join dim_info_cartao_pag b on a.info_cartao_pag_id = b.info_cartao_pag_id
    where 
        b.info_cartao_pag_id   is null
    {% endif %}	
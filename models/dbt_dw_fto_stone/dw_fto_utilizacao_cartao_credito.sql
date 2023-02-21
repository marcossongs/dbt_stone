{{
  config(
    materialized='incremental',
    unique_key='codigo_transacao',
    incremental_strategy='delete+insert',
    post_hook="{{ set_last_execution() }}; {{ dbt_model_grant() }}",
    dist='codigo_transacao',
    sort= 'codigo_transacao'
  )  
}}

select 
    codigo_transacao
    ,cast(data_hora_transacao as date) as data_transacao
    ,date_part(hour, data_hora_transacao) as hora_transacao
    ,md5(data_transacao+hora_transacao) as date_id
    ,date_part(dayofweek, data_hora_transacao) as dia_semana_int
    ,md5(metodo_captura) as metodo_captura_id    
    ,md5(bandeira_cartao+metodo_pagamento+estado_transacao) as inf_cartao_pag_id
    ,md5(codigo_usuario) cod_usuario_id
    ,data_hora_transacao as source_updated_dt
    ,getdate() load_date        
    ,sum(valor_transacao) as valor_transacao
from dbt_dw_stone.stg_stone
{% if is_incremental() %}
    where 
        source_updated_dt > '{{ get_max_event_time() }}'
{% endif %}
    group by 1,2,3,4,5,6,7,8,9,10
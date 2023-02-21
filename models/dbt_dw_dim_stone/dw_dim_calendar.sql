{{
  config(
    materialized='incremental',
    unique_key='date_id',
    incremental_strategy='delete+insert',
    post_hook="{{ set_last_execution() }}; {{ dbt_model_grant() }}",
    dist='date_id',
    sort= 'date_id'
  )  
}}


with 
	{% if is_incremental() %}
dim_calendar
as
(
 select date_id from {{this}}
),
    {% endif %}	
 atualiza_calendar
as
(
select distinct
    cast(data_hora_transacao as date) as data_transacao
    ,date_part(hour, data_hora_transacao) as hora_transacao
    ,md5(data_transacao+hora_transacao) as date_id
    ,date_part(dayofweek, data_hora_transacao) as dia_semana_int
    ,case 
        when dia_semana_int = 0 then 'Dom'
        when dia_semana_int = 1 then 'Seg'
        when dia_semana_int = 2 then 'Ter'
        when dia_semana_int = 3 then 'Qua'
        when dia_semana_int = 4 then 'Qui'
        when dia_semana_int = 5 then 'Sex'
        when dia_semana_int = 6 then 'Sab'
    else 'Indefinido' end             as dia_semana    
    ,getdate() as source_updated_dt
from dbt_dw_stone.stg_stone a
)
select a.* from atualiza_calendar a 
	{% if is_incremental() %}
    left join dim_calendar b on a.date_id = b.date_id
    where 
        b.date_id   is null
    {% endif %}	
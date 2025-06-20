def filter_by_localidade(cnes: int = None, equipe: int = None):
    where_clause = ""
    if cnes is not None:
        where_clause += f"""            where 
                    p.codigo_unidade_saude = {cnes}
                """
        if equipe and equipe is not None:
            where_clause += f"  and p.codigo_equipe_vinculada = {equipe} "

    return f"""WITH cidadaos as (
select
    distinct p.*,
    case 
        when LOWER(tipo_localidade) is null  then 'nao_definido'
        when LOWER(tipo_localidade) = 'rural' then 'rural'
        when LOWER(tipo_localidade) = 'urbana' then 'urbano'
    end tipo    
from
    pessoas p 
{where_clause})
select tipo, count(*) total  from cidadaos group by 1 """

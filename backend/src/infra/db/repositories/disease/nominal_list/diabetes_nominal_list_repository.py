# pylint: disable=R0913
from typing import Dict

import pandas as pd
from sqlalchemy import or_
from src.domain.entities.diabetes import Diabetes
from src.infra.db.entities.diabetes_nominal import DiabetesNominal
from src.infra.db.entities.equipes import Equipes
from src.infra.db.entities.pessoas import Pessoas
from src.infra.db.settings.connection_local import DBConnectionHandler

columns = [
    DiabetesNominal.co_fat_cidadao_pec,
    DiabetesNominal.diagnostico,
    DiabetesNominal.cids,
    DiabetesNominal.min_date,
    DiabetesNominal.data_ultima_visita_acs,
    DiabetesNominal.alerta_visita_acs,
    DiabetesNominal.total_consulta_individual_medico,
    DiabetesNominal.alerta_total_de_consultas_medico,
    DiabetesNominal.ultimo_atendimento_medico,
    DiabetesNominal.alerta_ultima_consulta_medico,
    DiabetesNominal.ultimo_atendimento_odonto,
    DiabetesNominal.alerta_ultima_consulta_odontologica,
    DiabetesNominal.ultima_data_afericao_pa,
    DiabetesNominal.alerta_afericao_pa,
    DiabetesNominal.ultima_data_hemoglobina_glicada,
    DiabetesNominal.alerta_ultima_hemoglobina_glicada,
    Pessoas.co_cidadao,
    Pessoas.raca_cor,
    Pessoas.cpf,
    Pessoas.cns,
    Pessoas.nome,
    Pessoas.nome_social,
    Pessoas.data_nascimento,
    Pessoas.idade,
    Pessoas.sexo,
    Pessoas.identidade_genero,
    Pessoas.telefone,
    Pessoas.ultima_atualizacao_cidadao,
    Pessoas.ultima_atualizacao_fcd,
    Pessoas.tipo_endereco,
    Pessoas.endereco,
    Pessoas.complemento,
    Pessoas.numero,
    Pessoas.bairro,
    Pessoas.cep,
    Pessoas.tipo_localidade,
    Equipes.nome_unidade_saude, Equipes.nome_equipe, Equipes.micro_area
]
class DiabetesNominalListRepository:

    def find_all(self, cnes: int = None) -> Dict:
        with DBConnectionHandler() as db_con:
            users = (
                db_con.session.query(DiabetesNominal)
                .join(
                    Pessoas,
                    Pessoas.cidadao_pec == DiabetesNominal.co_fat_cidadao_pec,
                    isouter=True,
                )
                .join(
                    Equipes,
                    Equipes.cidadao_pec == DiabetesNominal.co_fat_cidadao_pec,
                    isouter=True,
                )
                .filter(Equipes.codigo_unidade_saude == cnes)
                .all()
            )
            return users

    def find_all_download(self, cnes: int = None, equipe:int=None) -> Dict:
        where_clause = " "
        if cnes is not None and cnes:
            where_clause += f" where e.codigo_unidade_saude  = {cnes} "
            if equipe is not None and equipe:
                where_clause += f" and e.codigo_equipe  = {equipe} "
        with DBConnectionHandler() as db_con:
            response = pd.read_sql_query(
                con=db_con.get_engine(),
                sql=f"""select
	p.cidadao_pec as codigo_cidadao,
	p.nome  as nome,
	p.cns as cns,
	p.cpf as cpf,
	p.sexo as sexo,
	p.raca_cor  as "raca/cor",
	group_concat(e.micro_area) micro_area,
	group_concat(e.nome_equipe) nome_equipe,
	e.nome_unidade_saude,
	STRFTIME( '%d-%m-%Y',p.data_nascimento) data_nascimento,
	p.idade ,
	p.tipo_endereco ,
	p.endereco || ' ' || p.numero logradouro,
	p.complemento,
	p.bairro ,
	p.cep,
	p.tipo_localidade ,
	STRFTIME( '%d-%m-%Y',dn.min_date) primeiro_atendimento,
	dn.cids ,
	dn.ciaps ,
	dn.diagnostico 'grupo/condição',
    STRFTIME( '%d-%m-%Y',dn.data_ultima_visita_acs) data_ultima_visita_acs,
	case 
		when dn.alerta_visita_acs = 1 then 'SIM'
		when dn.alerta_visita_acs = 0 or dn.alerta_visita_acs  is null then 'NAO'
	end alerta_visita_acs ,
	dn.total_consulta_individual_medico ,
	dn.total_consulta_individual_enfermeiro,
	dn.total_consulta_individual_medico_enfermeiro total_de_consultas_medicas_enfermagem,
	case 
		when dn.alerta_total_de_consultas_medico = 1 then 'SIM'
		when dn.alerta_total_de_consultas_medico = 0 or dn.alerta_total_de_consultas_medico  is null then 'NAO'
	end   alerta_total_de_consultas_medicas_enfermagem,
    STRFTIME( '%d-%m-%Y',dn.ultimo_atendimento_medico_enfermeiro) data_ultima_consulta_medica_enfermagem,
    STRFTIME( '%d-%m-%Y',dn.ultimo_atendimento_odonto) ultimo_atendimento_odonto,
	case 
		when dn.alerta_ultima_consulta_odontologica = 1 then 'SIM'
		when dn.alerta_ultima_consulta_odontologica = 0 or dn.alerta_ultima_consulta_odontologica  is null then 'NAO'
	end  alerta_ultima_consulta_odontologica,
    STRFTIME( '%d-%m-%Y',dn.ultima_data_afericao_pa) ultima_data_afericao_pa,
	case 
		when dn.alerta_afericao_pa = 1 then 'SIM'
		when dn.alerta_afericao_pa = 0 or dn.alerta_afericao_pa  is null then 'NAO'
	end alerta_afericao_pa ,
    STRFTIME( '%d-%m-%Y',dn.ultima_data_hemoglobina_glicada) ultima_data_hemoglobina_glicada,
	case 
		when dn.alerta_ultima_hemoglobina_glicada = 1 then 'SIM'
		when dn.alerta_ultima_hemoglobina_glicada = 0 or dn.alerta_ultima_hemoglobina_glicada  is null then 'NAO'
	end alerta_ultima_hemoglobina_glicada 
from
	diabetes_nominal dn join pessoas p on p.cidadao_pec = dn.co_fat_cidadao_pec 
	left join equipes e on e.cidadao_pec  = p.cidadao_pec  
{where_clause}
group by p.cidadao_pec	
order by p.nome
""",
            )
            diabetes = Diabetes()
            response['cids'] = response['cids'].apply( lambda x: ", ".join(list(set(x.split("|")) & set(diabetes.target))))
            response['ciaps'] = response['ciaps'].apply( lambda x: ", ".join(list(set(x.split("|")) & set(diabetes.target))))

            return response

    def find_by_nome(self, nome: str):
        with DBConnectionHandler() as db_con:
            users = (
                db_con.session.query(DiabetesNominal)
                .join(
                    Pessoas,
                    Pessoas.cidadao_pec == DiabetesNominal.co_fat_cidadao_pec,
                )
                .filter(Pessoas.nome.ilike(f"%{nome}%"))
                .all()
            )
            return users

    def find_filter(
        self,
        cnes: int,
        page: int = 0,
        pagesize: int = 10,
        nome: str = None,
        cpf: str = None,
        equipe: int = None,
        query: str = None,
    ):
        page = int(page) if page is not None else 0
        pagesize = int(pagesize) if pagesize is not None else 0
        with DBConnectionHandler() as db_con:
            users = db_con.session.query(*columns).distinct(
                DiabetesNominal.co_fat_cidadao_pec
            )\
            .join(Pessoas, Pessoas.cidadao_pec == DiabetesNominal.co_fat_cidadao_pec,)\
            .join(Equipes, Equipes.cidadao_pec == DiabetesNominal.co_fat_cidadao_pec,)

            or_conditions, conditions = [], []
            if cnes is not None and cnes:
                conditions += [
                Equipes.codigo_unidade_saude == cnes
                ]
            if nome is not None and nome:
                conditions += [Pessoas.nome.ilike(f"%{nome}%")]
            if cpf is not None and cpf:
                conditions += [Pessoas.cpf.ilike(f"%{cpf}%")]

            if query is not None and query:
                or_conditions += [
                    Pessoas.cpf.ilike(f"%{query}%"),
                    Pessoas.cns.ilike(f"%{query}%"),
                    Pessoas.nome.ilike(f"%{query}%"),
                ]
            if equipe is not None and equipe:
                conditions += [Equipes.
                                     codigo_equipe == equipe]
            if len(conditions) > 0:
                users = users.filter(*conditions)

            if len(or_conditions) > 0:
                users = users.filter(or_(*or_conditions))

            users = users.filter(Pessoas.cidadao_pec.is_not(None))
            users = users.group_by(DiabetesNominal.co_fat_cidadao_pec)
            total = users.count()
            users = (
                users.order_by(Pessoas.nome)
                .offset(max(0, page - 1) * pagesize)
                .limit(pagesize)
            )
            # print(str(users))
            return {
                "itemsCount": total,
                "itemsPerPage": pagesize,
                "page": page,
                "pagesCount": round(total / pagesize),
                "items": list(users),
            }

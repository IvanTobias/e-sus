# /home/ubuntu/esus_project/backend/models/senha.py
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import enum

Base = declarative_base()

class StatusSenha(enum.Enum):
    AGUARDANDO = "Aguardando"
    CHAMADO = "Chamado"
    EM_ATENDIMENTO = "Em Atendimento"
    FINALIZADO = "Finalizado"
    CANCELADO = "Cancelado"

class Senha(Base):
    __tablename__ = 'senhas'

    id = Column(Integer, primary_key=True)
    numero_senha = Column(String(10), nullable=False) # Ex: P001, G002, N003
    tipo_atendimento = Column(String(50)) # Ex: Consulta Médica, Enfermagem, Odonto, Geral
    paciente_nome = Column(String(255), nullable=True)
    paciente_cpf = Column(String(11), nullable=True)
    paciente_cns = Column(String(15), nullable=True)
    unidade_id = Column(Integer, nullable=False) # Link to a future Unidade table if needed
    profissional_id = Column(Integer, nullable=True) # Link to a future Profissional table
    guiche_chamada = Column(String(10), nullable=True) # Ex: Guichê 1, Sala 2
    status = Column(SQLEnum(StatusSenha), default=StatusSenha.AGUARDANDO, nullable=False)
    dt_geracao = Column(DateTime, default=datetime.utcnow)
    dt_chamada = Column(DateTime, nullable=True)
    dt_inicio_atendimento = Column(DateTime, nullable=True)
    dt_fim_atendimento = Column(DateTime, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "numero_senha": self.numero_senha,
            "tipo_atendimento": self.tipo_atendimento,
            "paciente_nome": self.paciente_nome,
            "paciente_cpf": self.paciente_cpf,
            "paciente_cns": self.paciente_cns,
            "unidade_id": self.unidade_id,
            "profissional_id": self.profissional_id,
            "guiche_chamada": self.guiche_chamada,
            "status": self.status.value if self.status else None,
            "dt_geracao": self.dt_geracao.isoformat() if self.dt_geracao else None,
            "dt_chamada": self.dt_chamada.isoformat() if self.dt_chamada else None,
            "dt_inicio_atendimento": self.dt_inicio_atendimento.isoformat() if self.dt_inicio_atendimento else None,
            "dt_fim_atendimento": self.dt_fim_atendimento.isoformat() if self.dt_fim_atendimento else None,
        }

# You might need tables for Unidade and Profissional later
# class Unidade(Base):
#     __tablename__ = 'unidades'
#     id = Column(Integer, primary_key=True)
#     nome = Column(String(255), nullable=False)

# class Profissional(Base):
#     __tablename__ = 'profissionais'
#     id = Column(Integer, primary_key=True)
#     nome = Column(String(255), nullable=False)


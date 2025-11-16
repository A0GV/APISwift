from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from datetime import datetime

class TrasladoQueryParams(BaseModel):
    estado: Optional[str] = Field(None, description="Estado del traslado, e.g., 'solicitado', 'terminado', etc.")
    idAmbulancia: Optional[int] = Field(None, description="ID de la ambulancia")
    paciente: Optional[str] = Field(None, description="Nombre del paciente. Puede ser parcial")
    operador: Optional[str] = Field(None, description="Nombre del operador. Puede ser parcial")
    fechaInicio: Optional[datetime] = Field(None, description="Fecha de inicio en formato YYYY-MM-DD")
    fechaFin: Optional[datetime] = Field(None, description="Fecha de fin en formato YYYY-MM-DD")
    orPacienteOperador : Optional[bool] = Field(None, description="Permite buscar nombres en ambos paciente y operador")

    # Para validar el dato bajo el alias 'estado'. Itera sobre los valores posibles
    @field_validator('estado')
    def validar_estado(cls, value):
        valid_states = ["solicitado", "pendiente", "en proceso", "terminado", "cancelado"]
        if value.lower() not in valid_states:
            raise ValueError(f"Valor inválido para estado: {value}")
        return value.lower()

    @field_validator('idAmbulancia')
    def validar_id_ambulancia(cls, value):
        if value < 0:
            raise ValueError("idAmbulancia debe ser un número positivo")
        return value

    @field_validator('paciente', 'operador')
    def validar_string(cls, value, info):
        if not isinstance(value, str):
            raise ValueError(f"{info.name} debe ser una cadena de texto")
        return value

    @field_validator('fechaInicio', 'fechaFin')
    def validar_fecha(cls, value, info):
        if not isinstance(value, datetime):
            raise ValueError(f"{info.name} debe ser una fecha válida en formato YYYY-MM-DD")
        return value
    
    @model_validator(mode = "after")
    def validar_fechas_estado(cls, values):
        if(values.fechaInicio and values.fechaFin and values.fechaInicio > values.fechaFin):
            raise ValueError("La fecha de fin debe suceder después de la fecha de inicio")    

class NotificacionesOperador(BaseModel):
    idOperador: int = Field(..., description="ID del operador")
    limite: Optional[int] = Field(None, description="Indica cuantas notificaciones traer")

    @field_validator('limite')
    def validar_limites_notificaciones(cls, value):
        if value is None: 
            return 20
        if (value > 20):
            raise ValueError("No se pueden solicitar más de 20 notificaciones")
        return value
        
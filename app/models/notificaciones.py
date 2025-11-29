from pydantic import BaseModel, Field, field_validator
from typing import Optional

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
    
# Para coordi
class NotificacionesCoordi(BaseModel): 
    limit: Optional[int] = Field(None, description="Indica cuantas notificaciones traer")

    @field_validator('limit')
    def validar_limites_notificaciones(cls, value):
        if value is None: 
            return 10  # Default 10 notifs
        if value > 30:
            raise ValueError("No se pueden solicitar más de 30 notificaciones")
        return value
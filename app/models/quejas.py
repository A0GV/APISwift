from pydantic import BaseModel, Field, field_validator
from typing import Optional


class ActualizarQueja(BaseModel):
    idQueja: int = Field(..., description="ID de la queja a actualizar")

    @field_validator('idQueja')
    def validar_id_queja(cls, value):
        if value <= 0:
            raise ValueError("idQueja debe ser un número positivo")
        return value

    
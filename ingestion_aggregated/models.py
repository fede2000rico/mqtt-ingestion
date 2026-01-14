from pydantic import BaseModel, Field, ValidationError
from typing import Optional, Union

# --- INPUT MODELS (From MQTT) ---

class BaseMqttInput(BaseModel):
    machine_id: str

class RecipeInput(BaseMqttInput):
    recipe_id: str

class JobInput(BaseMqttInput):
    job_id: str

class BatchInput(BaseMqttInput):
    batch_id: str

class StatusInput(BaseMqttInput):
    status: str

# Telemetry/Alarm Input is more dynamic, but we can define a common base
class DataInput(BaseMqttInput):
    pass 
    # specific fields like temperature/speed are checked dynamically or via separate models if strictly defined

class MachineDataCombined(BaseModel):
    """
    Internal model representing the aggregated state of a machine.
    """
    machine_id: str
    recipe_id: Optional[str] = None
    job_id: Optional[str] = None
    batch_id: Optional[str] = None
    status: Optional[str] = None
    
    # Data fields (Optional until fully populated)
    temperature: Optional[float] = None
    speed: Optional[float] = None
    alarm_code: Optional[str] = None
    alarm_message: Optional[str] = None

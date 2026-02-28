from typing import Protocol, List, Dict, Any, runtime_checkable

@runtime_checkable
class DataSink(Protocol):
    """
    output abstraction rules
    """
    def write(self, results: Dict[str, Any]) -> None:
        ...

class PipelineService(Protocol):
    """i
    input abstraction rules
    """
    def execute(self, raw_data: List[Dict[str, Any]]) -> None:
        ...

from typing import Protocol, Dict, Any, runtime_checkable

#teletry rules

@runtime_checkable
class Observer(Protocol):
    # rules for the dashboard to get live queue updates
    def update(self, raw_qsize: int, processed_qsize: int) -> None:
        ...

@runtime_checkable
class Subject(Protocol):
    # rules for the monitor to connect and update the dashboard
    def attach(self, observer: Observer) -> None:
        ...

    def notify(self) -> None:
        ...


# core math rules

@runtime_checkable
class StatelessTask(Protocol):
    # for simple tasks that don't need to remember past rows (like checking signatures)
    def execute(self, packet: Dict[str, Any]) -> Dict[str, Any] | None:
        ...

@runtime_checkable
class StatefulTask(Protocol):
    # for tasks that need to remember past rows to do math (like running averages)
    def update_state(self, packet: Dict[str, Any]) -> Dict[str, Any] | None:
        ...

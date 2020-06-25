from abc import ABC, abstractmethod


class ProcessGraph(ABC):
    def __init__(self, process_graph_id: str):
        self.process_graph_id = process_graph_id

    @abstractmethod
    def update(self, process_graph: dict, **kwargs) -> 'ProcessGraph':
        """
        Updates an existing user-defined process (process graph) for the authenticated user.
        """
        pass

    @abstractmethod
    def describe(self) -> dict:
        """
        Returns all information about a user-defined process, including its process graph.
        """
        pass

    @abstractmethod
    def delete(self) -> None:
        """
        Deletes a user-defined process (process graph).
        """
        pass

    @abstractmethod
    def validate(self) -> None:
        """
        Validates a process graph without executing it; raises an error if validation fails.
        """
        pass

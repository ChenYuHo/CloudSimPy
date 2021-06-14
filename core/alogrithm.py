from abc import ABC, abstractmethod


class Placement(ABC):
    @abstractmethod
    def __call__(self, task, cluster, clock):
        pass

class Choose(ABC):
    @abstractmethod
    def __call__(self, cluster):
        pass

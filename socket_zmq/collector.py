"""Collector collect processed requests."""
import logging

from .utils import in_loop
from .mixin import LoopMixin

__all__ = ['Collector']

logger = logging.getLogger(__name__)


class Collector(LoopMixin):
    """Collect connections."""

    app = None

    def __init__(self):
        self.connections = set()

    def register(self, connection):
        """Register new connection."""
        self.connections.add(connection)

    def remove(self, connection):
        """Remove registered connection."""
        try:
            self.connections.remove(connection)
        except KeyError:
            logger.warning('Connection %r not registered', connection)

    @in_loop
    def start(self):
        pass

    @in_loop
    def stop(self):
        while self.connections:
            connection = self.connections.pop()
            if not connection.is_closed():
                connection.close()

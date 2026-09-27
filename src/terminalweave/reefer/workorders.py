from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from terminalweave.core.clock import ensure_utc, iso_z
from terminalweave.core.errors import ValidationError, StateTransitionError
from terminalweave.reefer.monitoring import ExcursionEpisode, ReeferMonitor

class ReeferWorkState(str, Enum):
    OPEN = 'open'
    ACK = 'ack'
    WORKING = 'working'
    CLOSED = 'closed'

@dataclass
class ReeferWorkOrder:
    order_id: str
    container_id: str
    opened_at: datetime
    reason: str
    severity: int
    state: ReeferWorkState = ReeferWorkState.OPEN
    assignee: str | None = None
    resolution: str | None = None

    def __post_init__(self):
        self.opened_at = ensure_utc(self.opened_at)
        if not 1 <= self.severity <= 5:
            raise ValidationError('severity must be 1..5')

class ReeferWorkBoard:

    def __init__(self):
        self.orders: dict[str, ReeferWorkOrder] = {}

    def open(self, o: ReeferWorkOrder) -> None:
        if o.order_id in self.orders:
            raise ValidationError('duplicate work order')
        self.orders[o.order_id] = o

    def open_once(self, o: ReeferWorkOrder) -> bool:
        """Open unless the order id already exists; returns True if opened."""
        if o.order_id in self.orders:
            return False
        self.orders[o.order_id] = o
        return True

    def acknowledge(self, order_id: str, assignee: str) -> ReeferWorkOrder:
        o = self.orders[order_id]
        if o.state != ReeferWorkState.OPEN:
            raise StateTransitionError('work order is not open')
        o.state = ReeferWorkState.ACK
        o.assignee = assignee
        return o

    def start(self, order_id: str) -> ReeferWorkOrder:
        o = self.orders[order_id]
        if o.state != ReeferWorkState.ACK:
            raise StateTransitionError('work order must be acknowledged')
        o.state = ReeferWorkState.WORKING
        return o

    def close(self, order_id: str, resolution: str) -> ReeferWorkOrder:
        o = self.orders[order_id]
        if o.state != ReeferWorkState.WORKING:
            raise StateTransitionError('work order must be in progress')
        if not resolution.strip():
            raise ValidationError('resolution required')
        o.state = ReeferWorkState.CLOSED
        o.resolution = resolution
        return o

    def backlog(self) -> tuple[ReeferWorkOrder, ...]:
        return tuple(sorted((o for o in self.orders.values() if o.state != ReeferWorkState.CLOSED), key=lambda o: (-o.severity, o.opened_at)))

class ReeferDispatcher:
    """Turns confirmed excursion episodes into work orders, exactly once.

    Order ids derive from the episode's event-time identity, so duplicate
    syncs, replayed telemetry and late readings can never raise a second
    order for the same excursion.
    """

    def __init__(self, container_id: str, monitor: ReeferMonitor, board: ReeferWorkBoard, severity: int=3, reason: str='temperature excursion'):
        if not container_id.strip():
            raise ValidationError('container id required')
        self.container_id = container_id
        self.monitor = monitor
        self.board = board
        self.severity = severity
        self.reason = reason
        self._issued: set[str] = set()

    def _order_id(self, episode: ExcursionEpisode) -> str:
        return f'reefer:{self.container_id}:{iso_z(episode.confirmed_at)}'

    def sync(self) -> tuple[ReeferWorkOrder, ...]:
        opened = []
        for episode in self.monitor.episodes:
            order_id = self._order_id(episode)
            if order_id in self._issued:
                continue
            self._issued.add(order_id)
            order = ReeferWorkOrder(order_id, self.container_id, episode.confirmed_at, self.reason, self.severity)
            if self.board.open_once(order):
                opened.append(order)
        return tuple(opened)

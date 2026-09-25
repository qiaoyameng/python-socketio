import itertools
import logging
import json

from bidict import bidict, ValueDuplicationError

default_logger = logging.getLogger('socketio')


class RoomSelection:
    """A recipient selection based on set operations over rooms.

    The selection is evaluated by the manager (or by each server in a
    pub/sub deployment) against the clients of a single namespace, so the
    application does not need to retrieve the full list of session IDs to
    compute the recipients itself.

    :param union: a room name or list of room names. Clients that are in at
                  least one of these rooms are candidates for the selection.
                  If omitted, all the clients connected to the namespace are
                  candidates.
    :param intersect: a room name or list of room names. Candidates are kept
                      only if they are in all of these rooms.
    :param exclude: a room name or list of room names. Clients that are in
                    any of these rooms are removed from the selection.

    At least one of the three arguments must be provided. Example usage to
    reach clients that are in the ``'project'`` room and also on duty, but
    not muted::

        sio.emit('notification', data,
                 to=socketio.RoomSelection(union='project',
                                           intersect='on-duty',
                                           exclude='muted'))
    """
    def __init__(self, union=None, intersect=None, exclude=None):
        self.union = self._validate_rooms(union, 'union')
        self.intersect = self._validate_rooms(intersect, 'intersect')
        self.exclude = self._validate_rooms(exclude, 'exclude')
        if self.union is None and self.intersect is None and \
                self.exclude is None:
            raise ValueError(
                'RoomSelection requires at least one of "union", '
                '"intersect" or "exclude".')

    @staticmethod
    def _validate_rooms(rooms, name):
        if rooms is None:
            return None
        if isinstance(rooms, str):
            rooms = [rooms]
        elif isinstance(rooms, dict):
            raise ValueError(
                'RoomSelection "{}" must be a room name or a list of '
                'room names.'.format(name))
        else:
            try:
                rooms = list(rooms)
            except TypeError:
                raise ValueError(
                    'RoomSelection "{}" must be a room name or a list of '
                    'room names.'.format(name))
        if len(rooms) == 0:
            return None
        for room in rooms:
            if not isinstance(room, str):
                raise ValueError(
                    'RoomSelection "{}" entries must be room name '
                    'strings.'.format(name))
        return rooms

    def to_dict(self):
        """Return a JSON-serializable representation of this selection."""
        return {
            'union': self.union,
            'intersect': self.intersect,
            'exclude': self.exclude,
        }

    @classmethod
    def from_dict(cls, selection):
        """Rebuild a selection from its dictionary representation."""
        return cls(union=selection.get('union'),
                   intersect=selection.get('intersect'),
                   exclude=selection.get('exclude'))

    def __eq__(self, other):
        return isinstance(other, RoomSelection) and \
            self.to_dict() == other.to_dict()

    def __repr__(self):
        return 'RoomSelection(union={!r}, intersect={!r}, exclude={!r})' \
            .format(self.union, self.intersect, self.exclude)


def resolve_room(to=None, room=None):
    """Combine the ``to`` and ``room`` arguments of an emit call.

    Conflicting combinations are rejected before anything is sent.
    """
    if to is not None and room is not None and to != room:
        raise ValueError(
            'The "to" and "room" arguments cannot be used together with '
            'different values.')
    return to if to is not None else room


class BaseManager:
    def __init__(self):
        self.logger = None
        self.server = None
        self.rooms = {}  # self.rooms[namespace][room][sio_sid] = eio_sid
        self.eio_to_sid = {}
        self.callbacks = {}
        self.pending_disconnect = {}
        self.json = json

    def set_server(self, server):
        self.server = server
        self.json = self.server.packet_class.json  # use the global JSON module

    def initialize(self):
        """Invoked before the first request is received. Subclasses can add
        their initialization code here.
        """
        pass

    def get_namespaces(self):
        """Return an iterable with the active namespace names."""
        return self.rooms.keys()

    def get_participants(self, namespace, room):
        """Return an iterable with the active participants in a room.

        The room can be given as a single room name, a list of room names
        (evaluated as a union) or a :class:`RoomSelection` instance, which
        is evaluated as a combination of union, intersection and exclusion
        operations over rooms. Each participant is returned at most once.

        Note that in a multi-server scenario this method only returns the
        participants connect to the server in which the method is called. There
        is currently no functionality to assemble a complete list of users
        across multiple servers.
        """
        ns = self.rooms.get(namespace, {})
        if isinstance(room, RoomSelection):
            # the selection is evaluated here, so that each participant is
            # returned only once, and so that the list of recipients is a
            # snapshot that is not affected by room changes that occur
            # while the event is being emitted
            participants = {}
            if room.union is not None:
                for r in room.union:
                    participants.update(ns.get(r, {}))
            else:
                participants.update(ns.get(None, {}))
            if room.intersect is not None:
                for r in room.intersect:
                    members = ns.get(r, {})
                    participants = {
                        sid: eio_sid
                        for sid, eio_sid in participants.items()
                        if sid in members
                    }
            if room.exclude is not None:
                excluded = set()
                for r in room.exclude:
                    excluded.update(ns.get(r, {}))
                participants = {
                    sid: eio_sid
                    for sid, eio_sid in participants.items()
                    if sid not in excluded
                }
        elif hasattr(room, '__len__') and not isinstance(room, str):
            participants = ns[room[0]]._fwdm.copy() if room[0] in ns else {}
            for r in room[1:]:
                participants.update(ns[r]._fwdm if r in ns else {})
        else:
            participants = ns[room]._fwdm.copy() if room in ns else {}
        yield from participants.items()

    def connect(self, eio_sid, namespace):
        """Register a client connection to a namespace."""
        sid = self.server.eio.generate_id()
        try:
            self.basic_enter_room(sid, namespace, None, eio_sid=eio_sid)
        except ValueDuplicationError:
            # already connected
            return None
        self.basic_enter_room(sid, namespace, sid, eio_sid=eio_sid)
        return sid

    def is_connected(self, sid, namespace):
        if namespace in self.pending_disconnect and \
                sid in self.pending_disconnect[namespace]:
            # the client is in the process of being disconnected
            return False
        try:
            return self.rooms[namespace][None][sid] is not None
        except KeyError:
            pass
        return False

    def sid_from_eio_sid(self, eio_sid, namespace):
        try:
            return self.rooms[namespace][None]._invm[eio_sid]
        except KeyError:
            pass

    def eio_sid_from_sid(self, sid, namespace):
        if namespace in self.rooms:
            return self.rooms[namespace][None].get(sid)

    def pre_disconnect(self, sid, namespace):
        """Put the client in the to-be-disconnected list.

        This allows the client data structures to be present while the
        disconnect handler is invoked, but still recognize the fact that the
        client is soon going away.
        """
        if namespace not in self.pending_disconnect:
            self.pending_disconnect[namespace] = []
        self.pending_disconnect[namespace].append(sid)
        return self.rooms[namespace][None].get(sid)

    def basic_disconnect(self, sid, namespace, **kwargs):
        if namespace not in self.rooms:
            return
        rooms = []
        for room_name, room in self.rooms[namespace].copy().items():
            if sid in room:
                rooms.append(room_name)
        for room in rooms:
            self.basic_leave_room(sid, namespace, room)
        if sid in self.callbacks:
            del self.callbacks[sid]
        if namespace in self.pending_disconnect and \
                sid in self.pending_disconnect[namespace]:
            self.pending_disconnect[namespace].remove(sid)
            if len(self.pending_disconnect[namespace]) == 0:
                del self.pending_disconnect[namespace]

    def is_sid_room(self, namespace, room):
        return room in self.rooms.get(namespace, {}).get(None, {})

    def basic_enter_room(self, sid, namespace, room, eio_sid=None):
        if eio_sid is None and namespace not in self.rooms:
            raise ValueError('sid is not connected to requested namespace')
        if sid != room and self.is_sid_room(namespace, room):
            raise ValueError('cannot enter a sid room')
        if namespace not in self.rooms:
            self.rooms[namespace] = {}
        if room not in self.rooms[namespace]:
            self.rooms[namespace][room] = bidict()
        if eio_sid is None:
            eio_sid = self.rooms[namespace][None][sid]
        self.rooms[namespace][room][sid] = eio_sid

    def basic_leave_room(self, sid, namespace, room):
        try:
            del self.rooms[namespace][room][sid]
            if len(self.rooms[namespace][room]) == 0:
                del self.rooms[namespace][room]
                if len(self.rooms[namespace]) == 0:
                    del self.rooms[namespace]
        except KeyError:
            pass

    def basic_close_room(self, room, namespace):
        if self.is_sid_room(namespace, room):
            raise ValueError('cannot close a sid room')
        try:
            for sid, _ in self.get_participants(namespace, room):
                self.basic_leave_room(sid, namespace, room)
        except KeyError:  # pragma: no cover
            pass

    def get_rooms(self, sid, namespace):
        """Return the rooms a client is in."""
        r = []
        try:
            for room_name, room in self.rooms[namespace].items():
                if room_name is not None and sid in room:
                    r.append(room_name)
        except KeyError:
            pass
        return r

    def _generate_ack_id(self, sid, callback):
        """Generate a unique identifier for an ACK packet."""
        if sid not in self.callbacks:
            self.callbacks[sid] = {0: itertools.count(1)}
        id = next(self.callbacks[sid][0])
        self.callbacks[sid][id] = callback
        return id

    def _get_logger(self):
        """Get the appropriate logger

        Prevents uninitialized servers in write-only mode from failing.
        """

        if self.logger:
            return self.logger
        elif self.server:
            return self.server.logger
        else:
            return default_logger

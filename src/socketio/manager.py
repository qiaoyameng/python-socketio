import logging

from engineio import packet as eio_packet
from . import base_manager
from . import packet

default_logger = logging.getLogger('socketio')


class Manager(base_manager.BaseManager):
    """Manage client connections.

    This class keeps track of all the clients and the rooms they are in, to
    support the broadcasting of messages. The data used by this class is
    stored in a memory structure, making it appropriate only for single process
    services. More sophisticated storage backends can be implemented by
    subclasses.
    """
    def can_disconnect(self, sid, namespace):
        return self.is_connected(sid, namespace)

    def emit(self, event, data, namespace, room=None, skip_sid=None,
             callback=None, to=None, **kwargs):
        """Emit a message to a single client, a room, or all the clients
        connected to the namespace."""
        room = to or room
        if namespace not in self.rooms:
            return
        if isinstance(data, tuple):
            # tuples are expanded to multiple arguments, everything else is
            # sent as a single argument
            data = list(data)
        elif data is not None:
            data = [data]
        else:
            data = []
        if not isinstance(skip_sid, list):
            skip_sid = [skip_sid]
        if not callback:
            # when callbacks aren't used the packets sent to each recipient are
            # identical, so they can be generated once and reused
            pkt = self.server.packet_class(
                packet.EVENT, namespace=namespace, data=[event] + data)
            encoded_packet = pkt.encode()
            if not isinstance(encoded_packet, list):
                encoded_packet = [encoded_packet]
            eio_pkt = [eio_packet.Packet(eio_packet.MESSAGE, p)
                       for p in encoded_packet]
            for sid, eio_sid in self.get_participants(namespace, room):
                if sid not in skip_sid:
                    for p in eio_pkt:
                        self.server._send_eio_packet(eio_sid, p)
        else:
            # callbacks are used, so each recipient must be sent a packet that
            # contains a unique callback id
            # note that callbacks when addressing a group of people are
            # implemented but not tested or supported
            for sid, eio_sid in self.get_participants(namespace, room):
                if sid not in skip_sid:  # pragma: no branch
                    id = self._generate_ack_id(sid, callback)
                    pkt = self.server.packet_class(
                        packet.EVENT, namespace=namespace, data=[event] + data,
                        id=id)
                    self.server._send_packet(eio_sid, pkt)

    def emit_with_ack(self, event, data, namespace, room=None, skip_sid=None,
                      callback=None):
        """Emit a message to a room or to all clients and register an
        individual callback for each recipient.

        The list of recipients is determined at the time of the call and
        does not change even if clients join or leave the room while the
        acknowledgements are pending.

        The callback is invoked as ``callback(sid, *args)`` each time a
        recipient acknowledges the event.

        Returns a dict that maps the sid of each recipient to the ack id
        assigned to it.
        """
        if namespace not in self.rooms:
            return {}
        if isinstance(data, tuple):
            # tuples are expanded to multiple arguments, everything else is
            # sent as a single argument
            data = list(data)
        elif data is not None:
            data = [data]
        else:
            data = []
        if not isinstance(skip_sid, list):
            skip_sid = [skip_sid]
        acks = {}
        for sid, eio_sid in self.get_participants(namespace, room):
            if sid in skip_sid:
                continue

            def ack_callback(*args, sid=sid):
                callback(sid, *args)

            id = self._generate_ack_id(sid, ack_callback)
            pkt = self.server.packet_class(
                packet.EVENT, namespace=namespace, data=[event] + data,
                id=id)
            self.server._send_packet(eio_sid, pkt)
            acks[sid] = id
        return acks

    def disconnect(self, sid, namespace, **kwargs):
        """Register a client disconnect from a namespace."""
        return self.basic_disconnect(sid, namespace)

    def enter_room(self, sid, namespace, room, eio_sid=None):
        """Add a client to a room."""
        return self.basic_enter_room(sid, namespace, room, eio_sid=eio_sid)

    def leave_room(self, sid, namespace, room):
        """Remove a client from a room."""
        return self.basic_leave_room(sid, namespace, room)

    def close_room(self, room, namespace):
        """Remove all participants from a room."""
        return self.basic_close_room(room, namespace)

    def trigger_callback(self, sid, id, data):
        """Invoke an application callback."""
        callback = None
        try:
            callback = self.callbacks[sid][id]
        except KeyError:
            # if we get an unknown callback we just ignore it
            self._get_logger().warning('Unknown callback received, ignoring.')
        else:
            del self.callbacks[sid][id]
        if callback is not None:
            callback(*data)

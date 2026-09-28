import collections
import secrets
import threading
import time

from engineio import packet as eio_packet

from . import async_manager
from . import manager
from . import packet


class SessionRecoveryMixin:
    """Short-term connection recovery support for client managers.

    This mixin implements an optional, in-memory recovery layer on top of a
    regular client manager. It is designed for clients on unreliable networks
    (for example mobile devices switching between Wi-Fi and cellular) that
    need to rejoin their rooms and receive the events they missed while the
    transport was down.

    The recovery credential is a random token that is independent from the
    public session id (``sid``), so knowing a ``sid`` is not enough to take
    over a session. Tokens expire shortly after the client disconnects and
    are single-use: the first recovery attempt that presents a valid token
    wins and the token is rotated, so any concurrent or repeated attempt
    with the old token is rejected.

    Only recoverable events are recorded: plain ``EVENT`` packets emitted
    without an acknowledgement callback. Events that expect an ACK and
    connection lifecycle packets (CONNECT/DISCONNECT) are never replayed.

    The per-session buffer is bounded both in number of events
    (``buffer_max_events``) and in age (``buffer_max_age`` seconds). If the
    events a client missed have already been evicted, recovery reports a
    gap instead of silently skipping the missing events.

    Note that recovery is not available for servers that use the
    ``always_connect`` option.

    Note: this class is not meant to be used directly. Use
    :class:`RecoveryManager` or :class:`AsyncRecoveryManager` instead.
    """
    recovery_enabled = True

    def _init_recovery(self, recovery_timeout=60.0, buffer_max_events=100,
                       buffer_max_age=None, clock=None):
        """Initialize the recovery state.

        :param recovery_timeout: how many seconds a recovery token remains
                                 valid after the client disconnects.
        :param buffer_max_events: maximum number of events kept in the
                                  per-session recovery buffer.
        :param buffer_max_age: maximum age in seconds of a buffered event.
                               Defaults to ``recovery_timeout``.
        :param clock: a callable returning the current time in seconds.
                      Defaults to ``time.monotonic``. Tests can inject a
                      controlled clock.
        """
        self.recovery_timeout = recovery_timeout
        self.buffer_max_events = buffer_max_events
        self.buffer_max_age = buffer_max_age \
            if buffer_max_age is not None else recovery_timeout
        self.clock = clock or time.monotonic
        # token -> session dict with keys: namespace, sid, rooms, offset,
        # buffer (deque of (offset, [eio packets], timestamp)),
        # disconnected_at
        self._recovery_sessions = {}
        # (namespace, sid) -> token
        self._recovery_by_sid = {}
        self._recovery_lock = threading.Lock()

    def purge_expired_sessions(self):
        """Drop recovery sessions whose token has expired."""
        with self._recovery_lock:
            self._purge_expired(self.clock())

    def _purge_expired(self, now):
        expired = [token for token, session in self._recovery_sessions.items()
                   if session['disconnected_at'] is not None and
                   now - session['disconnected_at'] > self.recovery_timeout]
        for token in expired:
            session = self._recovery_sessions.pop(token)
            self._recovery_by_sid.pop(
                (session['namespace'], session['sid']), None)

    def _create_session(self, sid, namespace):
        now = self.clock()
        with self._recovery_lock:
            token = secrets.token_urlsafe(32)
            self._recovery_sessions[token] = {
                'namespace': namespace,
                'sid': sid,
                'rooms': [],
                'offset': 0,
                'buffer': collections.deque(),
                'disconnected_at': None,
            }
            self._recovery_by_sid[(namespace, sid)] = token
            return token

    def _trim_buffer(self, session, now):
        buffer = session['buffer']
        while buffer and now - buffer[0][2] > self.buffer_max_age:
            buffer.popleft()
        while len(buffer) > self.buffer_max_events:
            buffer.popleft()

    def _record_emit(self, namespace, data, room, skip_sid):
        """Buffer a recoverable event for the matching sessions."""
        now = self.clock()
        pkt = self.server.packet_class(
            packet.EVENT, namespace=namespace, data=data)
        encoded_packet = pkt.encode()
        if not isinstance(encoded_packet, list):
            encoded_packet = [encoded_packet]
        eio_packets = [eio_packet.Packet(eio_packet.MESSAGE, p)
                       for p in encoded_packet]
        if not isinstance(skip_sid, list):
            skip_sid = [skip_sid]
        skip_sid = set(skip_sid)
        if room is None:
            rooms = None
        elif isinstance(room, (list, tuple)):
            rooms = list(room)
        else:
            rooms = [room]
        with self._recovery_lock:
            self._purge_expired(now)
            sessions = []
            if rooms is None:
                # namespace broadcast: every session in the namespace
                sessions = [s for s in self._recovery_sessions.values()
                            if s['namespace'] == namespace]
            else:
                # connected recipients
                for sid, _ in self.get_participants(namespace, room):
                    token = self._recovery_by_sid.get((namespace, sid))
                    if token is not None:
                        sessions.append(self._recovery_sessions[token])
                # disconnected sessions that were in the room
                for session in self._recovery_sessions.values():
                    if session['namespace'] != namespace or \
                            session['disconnected_at'] is None:
                        continue
                    if any(r == session['sid'] or r in session['rooms']
                           for r in rooms):
                        sessions.append(session)
            for session in sessions:
                if session['sid'] in skip_sid:
                    continue
                session['offset'] += 1
                session['buffer'].append(
                    (session['offset'], eio_packets, now))
                self._trim_buffer(session, now)

    def _snapshot_disconnect(self, sid, namespace):
        """Save the rooms of a disconnecting client for later recovery."""
        now = self.clock()
        with self._recovery_lock:
            token = self._recovery_by_sid.get((namespace, sid))
            if token is None:
                return
            session = self._recovery_sessions.get(token)
            if session is None:
                return
            session['rooms'] = [r for r in self.get_rooms(sid, namespace)
                                if r != sid]
            session['disconnected_at'] = now

    def recover_session(self, sid, namespace, auth):
        """Attempt to recover a session for a newly connected client.

        This method is invoked by the server after a client connects. It
        always returns the recovery credentials for the new connection. If
        the client provided valid recovery credentials in its authentication
        payload, the previous session (rooms, event buffer and offset) is
        transferred to the new connection.

        :param sid: the session id of the new connection.
        :param namespace: the namespace of the connection.
        :param auth: the authentication payload sent by the client, or
                     ``None``.

        The return value is a dictionary with the following keys:

        - ``token``: the recovery token for the new connection.
        - ``offset``: the current event offset of the session.
        - ``recovered``: ``True`` if a previous session was recovered.
        - ``reason``: ``None``, or one of ``'invalid'``, ``'expired'`` or
          ``'gap'`` when recovery was requested but failed.
        - ``missed``: the list of Engine.IO packets to replay to the
          client, in their original order. Only present events are
          included; when a gap is detected this list is empty and
          ``recovered`` is ``False``, so a partial recovery is never
          reported as successful.
        """
        now = self.clock()
        result = {'recovered': False, 'reason': None, 'missed': []}
        with self._recovery_lock:
            token = self._recovery_by_sid.get((namespace, sid))
            session = self._recovery_sessions[token]
            request = auth.get('recovery') if isinstance(auth, dict) \
                else None
            if isinstance(request, dict):
                old_token = request.get('token')
                old_session = self._recovery_sessions.get(old_token)
                if old_session is not None and \
                        old_session['disconnected_at'] is not None and \
                        now - old_session['disconnected_at'] > \
                        self.recovery_timeout:
                    # expired tokens are consumed and cannot be reused
                    self._drop_session(old_token)
                    old_session = None
                    result['reason'] = 'expired'
                if old_session is None:
                    result['reason'] = result['reason'] or 'invalid'
                elif old_session['namespace'] != namespace or \
                        old_session['disconnected_at'] is None:
                    # a session that is still connected cannot be taken
                    # over, even with a valid token
                    result['reason'] = 'invalid'
                else:
                    offset = request.get('offset')
                    if not isinstance(offset, int) or \
                            isinstance(offset, bool) or offset < 0 or \
                            offset > old_session['offset']:
                        result['reason'] = 'invalid'
                    else:
                        # the token is single-use: the first valid recovery
                        # attempt consumes it and wins over any concurrent
                        # or repeated attempt
                        self._drop_session(old_token)
                        self._trim_buffer(old_session, now)
                        buffer = old_session['buffer']
                        if old_session['offset'] > offset and (
                                not buffer or buffer[0][0] > offset + 1):
                            # some of the missed events are gone from the
                            # buffer, so a complete recovery is impossible
                            result['reason'] = 'gap'
                        else:
                            session['offset'] = old_session['offset']
                            session['buffer'] = buffer
                            for room in old_session['rooms']:
                                try:
                                    self.basic_enter_room(
                                        sid, namespace, room)
                                except ValueError:
                                    pass
                            result['recovered'] = True
                            result['missed'] = [
                                p for off, packets, _ in buffer
                                if off > offset for p in packets]
            self._purge_expired(now)
            result['token'] = token
            result['offset'] = session['offset']
            return result

    def _drop_session(self, token):
        session = self._recovery_sessions.pop(token, None)
        if session is not None:
            self._recovery_by_sid.pop(
                (session['namespace'], session['sid']), None)


class RecoveryManager(SessionRecoveryMixin, manager.Manager):
    """In-memory client manager with short-term connection recovery.

    This manager behaves exactly like :class:`socketio.Manager`, with the
    addition of the optional connection recovery layer described in
    :class:`SessionRecoveryMixin`. Recovery is only active for servers that
    use this manager; the default manager does not change its behavior in
    any way.

    :param recovery_timeout: how many seconds a recovery token remains
                             valid after the client disconnects. The
                             default is 60 seconds.
    :param buffer_max_events: maximum number of events kept in the
                              per-session recovery buffer. The default is
                              100 events.
    :param buffer_max_age: maximum age in seconds of a buffered event.
                           Defaults to ``recovery_timeout``.
    :param clock: a callable returning the current time in seconds, used
                  to inject a controlled clock in tests.
    """
    def __init__(self, recovery_timeout=60.0, buffer_max_events=100,
                 buffer_max_age=None, clock=None):
        super().__init__()
        self._init_recovery(
            recovery_timeout=recovery_timeout,
            buffer_max_events=buffer_max_events,
            buffer_max_age=buffer_max_age, clock=clock)

    def connect(self, eio_sid, namespace):
        sid = super().connect(eio_sid, namespace)
        if sid is not None:
            self._create_session(sid, namespace)
        return sid

    def emit(self, event, data, namespace, room=None, skip_sid=None,
             callback=None, to=None, **kwargs):
        super().emit(event, data, namespace, room=room, skip_sid=skip_sid,
                     callback=callback, to=to, **kwargs)
        if callback is None:
            # only plain events are recoverable; events that expect an ACK
            # callback are never recorded or replayed
            if isinstance(data, tuple):
                data = list(data)
            elif data is not None:
                data = [data]
            else:
                data = []
            self._record_emit(namespace, [event] + data, to or room,
                              skip_sid)

    def disconnect(self, sid, namespace, **kwargs):
        self._snapshot_disconnect(sid, namespace)
        return super().disconnect(sid, namespace, **kwargs)


class AsyncRecoveryManager(SessionRecoveryMixin, async_manager.AsyncManager):
    """In-memory asyncio client manager with connection recovery.

    Equivalent to :class:`RecoveryManager`, for asyncio servers. See
    :class:`SessionRecoveryMixin` for details on the recovery behavior.

    :param recovery_timeout: how many seconds a recovery token remains
                             valid after the client disconnects. The
                             default is 60 seconds.
    :param buffer_max_events: maximum number of events kept in the
                              per-session recovery buffer. The default is
                              100 events.
    :param buffer_max_age: maximum age in seconds of a buffered event.
                           Defaults to ``recovery_timeout``.
    :param clock: a callable returning the current time in seconds, used
                  to inject a controlled clock in tests.
    """
    def __init__(self, recovery_timeout=60.0, buffer_max_events=100,
                 buffer_max_age=None, clock=None):
        super().__init__()
        self._init_recovery(
            recovery_timeout=recovery_timeout,
            buffer_max_events=buffer_max_events,
            buffer_max_age=buffer_max_age, clock=clock)

    async def connect(self, eio_sid, namespace):
        sid = await super().connect(eio_sid, namespace)
        if sid is not None:
            self._create_session(sid, namespace)
        return sid

    async def emit(self, event, data, namespace, room=None, skip_sid=None,
                   callback=None, to=None, **kwargs):
        await super().emit(event, data, namespace, room=room,
                           skip_sid=skip_sid, callback=callback, to=to,
                           **kwargs)
        if callback is None:
            # only plain events are recoverable; events that expect an ACK
            # callback are never recorded or replayed
            if isinstance(data, tuple):
                data = list(data)
            elif data is not None:
                data = [data]
            else:
                data = []
            self._record_emit(namespace, [event] + data, to or room,
                              skip_sid)

    async def disconnect(self, sid, namespace, **kwargs):
        self._snapshot_disconnect(sid, namespace)
        return await super().disconnect(sid, namespace, **kwargs)

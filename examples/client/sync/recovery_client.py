"""Client-side example of the connection recovery protocol.

This example shows how a client can survive short network interruptions
(for example a mobile device switching networks) by recovering its
server-side session, its rooms and the events it missed while offline.
The server must use ``socketio.RecoveryManager`` (or
``socketio.AsyncRecoveryManager``) for this to work; with the default
manager the protocol is inactive and connections behave as before.

Wire protocol
-------------

1. On every connection the server's CONNECT packet includes recovery
   credentials alongside the public ``sid``::

       0{"sid":"...",
         "recovery":{"token":"<secret>","offset":0,"recovered":false}}

   The ``token`` is a secret credential, independent from the ``sid``.
   Knowing a ``sid`` alone is not enough to take over a session.

2. The client counts the recoverable events it receives. Recoverable
   events are plain EVENT packets (packet type 2) without an
   acknowledgement id. Events with an ACK id and connection lifecycle
   packets are not recorded by the server and must not be counted.

3. After an unplanned disconnection, the client reconnects and passes
   its last confirmed offset in the auth payload::

       0{"recovery":{"token":"<secret>","offset":17}}

4. The server answers with a CONNECT packet whose ``recovery`` object
   reports the outcome, and then replays the missed events in their
   original order::

       0{"sid":"...",
         "recovery":{"token":"<new-secret>","offset":20,"recovered":true}}
       2["news",{"n":18}]
       2["news",{"n":19}]
       2["news",{"n":20}]

   The token is single-use and is rotated on every successful recovery,
   so a concurrent or repeated attempt with the old token is rejected.

5. If recovery is not possible, ``recovered`` is ``false`` and a
   ``reason`` is included:

   - ``"invalid"``: unknown or already consumed token, or bad offset.
   - ``"expired"``: the token was valid but its lifetime has passed.
   - ``"gap"``: some of the missed events were already evicted from the
     server buffer, so the session cannot be fully recovered. The client
     must resynchronize its state from scratch; the server never skips
     missing events and claims success.
"""
import socketio


class RecoveryClient(socketio.Client):
    """A Socket.IO client with support for connection recovery."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.recovery_token = None
        self.recovery_offset = 0
        self.session_recovered = False

    def connect(self, url, *args, **kwargs):
        auth = dict(kwargs.pop('auth', None) or {})
        if self.recovery_token is not None:
            # ask the server to recover the previous session
            auth['recovery'] = {
                'token': self.recovery_token,
                'offset': self.recovery_offset,
            }
        super().connect(url, *args, auth=auth, **kwargs)

    def _handle_connect(self, namespace, data):
        recovery = (data or {}).get('recovery')
        if recovery is not None:
            self.recovery_token = recovery['token']
            self.recovery_offset = recovery['offset']
            self.session_recovered = recovery.get('recovered', False)
            if not self.session_recovered and recovery.get('reason'):
                # recovery failed ('invalid', 'expired' or 'gap'):
                # resynchronize the application state from scratch here
                pass
        super()._handle_connect(namespace, data)

    def _handle_event(self, namespace, id, data):
        if id is None and self.recovery_token is not None:
            # plain events are recoverable, so advance the confirmed
            # offset; events with an ACK id are not recorded by the
            # server and must not be counted
            self.recovery_offset += 1
        super()._handle_event(namespace, id, data)


sio = RecoveryClient()


@sio.event
def connect():
    if sio.session_recovered:
        print('session recovered, missed events are being replayed')
    else:
        print('connected with a fresh session')


@sio.event
def disconnect(reason):
    print('disconnected:', reason)


@sio.on('news')
def on_news(data):
    print('news:', data)


if __name__ == '__main__':
    sio.connect('http://localhost:5000')
    try:
        sio.wait()
    except KeyboardInterrupt:
        pass
    finally:
        if sio.connected:
            sio.disconnect()

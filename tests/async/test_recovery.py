from unittest import mock

from engineio import json

from socketio import async_manager
from socketio import async_server
from socketio import packet
from socketio import recovery_manager


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def decode(eio_pkt):
    return packet.Packet(encoded_packet=eio_pkt.data).data


class TestAsyncRecoveryManager:
    def setup_method(self):
        id = 0

        def generate_id():
            nonlocal id
            id += 1
            return str(id)

        mock_server = mock.MagicMock()
        mock_server._send_packet = mock.AsyncMock()
        mock_server._send_eio_packet = mock.AsyncMock()
        mock_server.eio.generate_id = generate_id
        mock_server.packet_class = packet.Packet
        self.clock = FakeClock()
        self.mgr = recovery_manager.AsyncRecoveryManager(
            recovery_timeout=30, buffer_max_events=3, buffer_max_age=10,
            clock=self.clock)
        self.mgr.set_server(mock_server)
        self.mgr.initialize()

    async def connect(self, eio_sid, namespace='/', auth=None):
        sid = await self.mgr.connect(eio_sid, namespace)
        return sid, self.mgr.recover_session(sid, namespace, auth)

    async def test_disconnect_and_recover(self):
        sid, creds = await self.connect('eio1')
        await self.mgr.enter_room(sid, '/', 'chat')
        await self.mgr.emit('news', 'a', '/')
        await self.mgr.emit('news', 'b', '/')
        await self.mgr.disconnect(sid, '/')
        # events emitted while the client is offline are also buffered
        await self.mgr.emit('news', 'c', '/')
        await self.mgr.emit('news', 'd', '/', room='chat')

        sid2, r = await self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 1}})
        assert r['recovered'] is True
        assert r['reason'] is None
        assert r['token'] != creds['token']  # the token is rotated
        assert r['offset'] == 4
        assert [decode(p) for p in r['missed']] == [
            ['news', 'b'], ['news', 'c'], ['news', 'd']]
        # rooms are restored on the new sid
        assert 'chat' in self.mgr.get_rooms(sid2, '/')
        assert self.mgr.is_connected(sid2, '/')

    async def test_replay_order(self):
        sid, creds = await self.connect('eio1')
        for i in range(3):
            await self.mgr.emit('news', i, '/')
        await self.mgr.disconnect(sid, '/')
        for i in range(3, 5):
            await self.mgr.emit('news', i, '/')

        _, r = await self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 2}})
        assert r['recovered'] is True
        assert [decode(p) for p in r['missed']] == [
            ['news', 2], ['news', 3], ['news', 4]]

    async def test_expired_token(self):
        sid, creds = await self.connect('eio1')
        await self.mgr.disconnect(sid, '/')
        self.clock.advance(31)  # recovery_timeout is 30 seconds

        _, r = await self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 0}})
        assert r['recovered'] is False
        assert r['reason'] == 'expired'
        # the expired token is consumed and cannot be tried again
        _, r2 = await self.connect('eio3', auth={'recovery': {
            'token': creds['token'], 'offset': 0}})
        assert r2['recovered'] is False
        assert r2['reason'] == 'invalid'

    async def test_buffer_overflow_causes_gap(self):
        sid, creds = await self.connect('eio1')
        for i in range(5):  # buffer_max_events is 3, offsets 1-2 are lost
            await self.mgr.emit('news', i, '/')
        await self.mgr.disconnect(sid, '/')

        _, r = await self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 1}})
        # event 2 was evicted, so the session cannot be fully recovered
        assert r['recovered'] is False
        assert r['reason'] == 'gap'
        assert r['missed'] == []  # no partial replay is claimed as success
        assert r['offset'] == 0  # a fresh session starts over

    async def test_ack_events_are_not_recorded(self):
        sid, creds = await self.connect('eio1')
        await self.mgr.emit('news', 'a', '/',
                            callback=lambda *args: None)
        await self.mgr.emit('news', 'b', '/')
        await self.mgr.disconnect(sid, '/')

        _, r = await self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 0}})
        assert r['recovered'] is True
        assert r['offset'] == 1
        assert [decode(p) for p in r['missed']] == [['news', 'b']]

    async def test_double_recovery_first_one_wins(self):
        sid, creds = await self.connect('eio1')
        await self.mgr.emit('news', 'a', '/')
        await self.mgr.disconnect(sid, '/')
        auth = {'recovery': {'token': creds['token'], 'offset': 0}}

        _, r1 = await self.connect('eio2', auth=auth)
        _, r2 = await self.connect('eio3', auth=auth)
        assert r1['recovered'] is True
        assert r2['recovered'] is False
        assert r2['reason'] == 'invalid'

    def test_default_async_manager_has_no_recovery(self):
        assert not getattr(async_manager.AsyncManager(),
                           'recovery_enabled', False)


@mock.patch('socketio.server.engineio.AsyncServer', **{
    'return_value.generate_id.side_effect': [str(i) for i in range(1, 20)],
    'return_value.send_packet': mock.AsyncMock()})
class TestAsyncRecoveryServer:
    def make_server(self):
        clock = FakeClock()
        mgr = recovery_manager.AsyncRecoveryManager(
            recovery_timeout=30, buffer_max_events=3, buffer_max_age=10,
            clock=clock)
        s = async_server.AsyncServer(client_manager=mgr,
                                     async_handlers=False)
        s.on('connect', mock.AsyncMock())
        return s

    async def connect(self, s, eio_sid, auth=None):
        await s._handle_eio_connect(eio_sid, 'environ')
        if auth is None:
            await s._handle_eio_message(eio_sid, '0')
        else:
            await s._handle_eio_message(eio_sid, '0' + json.dumps(auth))
        pkt = s.eio.send.call_args[0][1]
        assert pkt[0] == '0'
        return json.loads(pkt[1:])

    async def test_connect_includes_recovery_credentials(self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = self.make_server()
        data = await self.connect(s, '123')
        assert data['sid'] == '1'
        assert data['recovery']['recovered'] is False
        assert data['recovery']['offset'] == 0
        assert data['recovery']['token']
        assert 'reason' not in data['recovery']

    async def test_recovery_flow(self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = self.make_server()
        data = await self.connect(s, '123')
        token = data['recovery']['token']
        await s.emit('news', 'a')
        await s.emit('news', 'b')
        await s._handle_disconnect('123', '/')
        await s.emit('news', 'c')
        s.eio.send_packet.reset_mock()

        data = await self.connect(s, '456', auth={'recovery': {
            'token': token, 'offset': 1}})
        assert data['recovery']['recovered'] is True
        assert data['recovery']['offset'] == 3
        assert data['recovery']['token'] != token
        # the missed events are replayed after the CONNECT packet
        replayed = [decode(c[0][1]) for c in
                    s.eio.send_packet.call_args_list]
        assert replayed == [['news', 'b'], ['news', 'c']]

    async def test_default_server_ignores_recovery(self, eio):
        eio.return_value.send = mock.AsyncMock()
        s = async_server.AsyncServer(async_handlers=False)
        s.on('connect', mock.AsyncMock())
        await s._handle_eio_connect('123', 'environ')
        await s._handle_eio_message(
            '123', '0{"recovery":{"token":"x","offset":0}}')
        # without a recovery manager the CONNECT packet is unchanged
        s.eio.send.assert_called_once_with('123', '0{"sid":"1"}')

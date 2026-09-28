import threading
from unittest import mock

from engineio import json

from socketio import manager
from socketio import packet
from socketio import recovery_manager
from socketio import server


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def decode(eio_pkt):
    return packet.Packet(encoded_packet=eio_pkt.data).data


class TestRecoveryManager:
    def setup_method(self):
        id = 0

        def generate_id():
            nonlocal id
            id += 1
            return str(id)

        mock_server = mock.MagicMock()
        mock_server.eio.generate_id = generate_id
        mock_server.packet_class = packet.Packet
        self.clock = FakeClock()
        self.mgr = recovery_manager.RecoveryManager(
            recovery_timeout=30, buffer_max_events=3, buffer_max_age=10,
            clock=self.clock)
        self.mgr.set_server(mock_server)
        self.mgr.initialize()

    def connect(self, eio_sid, namespace='/', auth=None):
        sid = self.mgr.connect(eio_sid, namespace)
        return sid, self.mgr.recover_session(sid, namespace, auth)

    def test_disconnect_and_recover(self):
        sid, creds = self.connect('eio1')
        assert creds['recovered'] is False
        assert creds['reason'] is None
        assert creds['offset'] == 0
        assert creds['token']
        self.mgr.enter_room(sid, '/', 'chat')
        self.mgr.emit('news', 'a', '/')
        self.mgr.emit('news', 'b', '/')
        self.mgr.disconnect(sid, '/')
        # events emitted while the client is offline are also buffered
        self.mgr.emit('news', 'c', '/')
        self.mgr.emit('news', 'd', '/', room='chat')

        sid2, r = self.connect('eio2', auth={'recovery': {
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

    def test_replay_order(self):
        sid, creds = self.connect('eio1')
        for i in range(3):
            self.mgr.emit('news', i, '/')
        self.mgr.disconnect(sid, '/')
        for i in range(3, 5):
            self.mgr.emit('news', i, '/')

        _, r = self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 2}})
        assert r['recovered'] is True
        assert [decode(p) for p in r['missed']] == [
            ['news', 2], ['news', 3], ['news', 4]]

    def test_recover_with_current_offset_replays_nothing(self):
        sid, creds = self.connect('eio1')
        self.mgr.emit('news', 'a', '/')
        self.mgr.disconnect(sid, '/')

        _, r = self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 1}})
        assert r['recovered'] is True
        assert r['missed'] == []
        assert r['offset'] == 1

    def test_expired_token(self):
        sid, creds = self.connect('eio1')
        self.mgr.disconnect(sid, '/')
        self.clock.advance(31)  # recovery_timeout is 30 seconds

        _, r = self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 0}})
        assert r['recovered'] is False
        assert r['reason'] == 'expired'
        # the expired token is consumed and cannot be tried again
        _, r2 = self.connect('eio3', auth={'recovery': {
            'token': creds['token'], 'offset': 0}})
        assert r2['recovered'] is False
        assert r2['reason'] == 'invalid'

    def test_unknown_token(self):
        _, r = self.connect('eio1', auth={'recovery': {
            'token': 'no-such-token', 'offset': 0}})
        assert r['recovered'] is False
        assert r['reason'] == 'invalid'

    def test_connected_session_cannot_be_recovered(self):
        sid, creds = self.connect('eio1')
        _, r = self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 0}})
        assert r['recovered'] is False
        assert r['reason'] == 'invalid'
        assert self.mgr.is_connected(sid, '/')

    def test_buffer_overflow_causes_gap(self):
        sid, creds = self.connect('eio1')
        for i in range(5):  # buffer_max_events is 3, offsets 1-2 are lost
            self.mgr.emit('news', i, '/')
        self.mgr.disconnect(sid, '/')

        _, r = self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 1}})
        # events 2 was evicted, so the session cannot be fully recovered
        assert r['recovered'] is False
        assert r['reason'] == 'gap'
        assert r['missed'] == []  # no partial replay is claimed as success
        assert r['offset'] == 0  # a fresh session starts over

    def test_buffer_overflow_within_bounds_recovers(self):
        sid, creds = self.connect('eio1')
        for i in range(5):
            self.mgr.emit('news', i, '/')
        self.mgr.disconnect(sid, '/')

        _, r = self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 2}})
        assert r['recovered'] is True
        assert [decode(p) for p in r['missed']] == [
            ['news', 2], ['news', 3], ['news', 4]]

    def test_buffer_max_age_causes_gap(self):
        sid, creds = self.connect('eio1')
        self.mgr.emit('news', 'old', '/')
        self.clock.advance(11)  # buffer_max_age is 10 seconds
        self.mgr.emit('news', 'new', '/')  # trims the aged out event
        self.mgr.disconnect(sid, '/')

        _, r = self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 0}})
        assert r['recovered'] is False
        assert r['reason'] == 'gap'

        sid3, creds3 = self.connect('eio3')
        self.mgr.emit('news', 'x', '/')
        self.mgr.disconnect(sid3, '/')
        _, r3 = self.connect('eio4', auth={'recovery': {
            'token': creds3['token'], 'offset': 0}})
        assert r3['recovered'] is True

    def test_ack_events_are_not_recorded(self):
        sid, creds = self.connect('eio1')
        self.mgr.emit('news', 'a', '/', callback=lambda *args: None)
        self.mgr.emit('news', 'b', '/')
        self.mgr.disconnect(sid, '/')

        _, r = self.connect('eio2', auth={'recovery': {
            'token': creds['token'], 'offset': 0}})
        assert r['recovered'] is True
        assert r['offset'] == 1
        assert [decode(p) for p in r['missed']] == [['news', 'b']]

    def test_skip_sid_is_not_recorded(self):
        sid, creds = self.connect('eio1')
        self.connect('eio2')
        self.mgr.emit('news', 'a', '/', skip_sid=sid)
        self.mgr.disconnect(sid, '/')

        _, r = self.connect('eio3', auth={'recovery': {
            'token': creds['token'], 'offset': 0}})
        assert r['recovered'] is True
        assert r['missed'] == []

    def test_double_recovery_first_one_wins(self):
        sid, creds = self.connect('eio1')
        self.mgr.emit('news', 'a', '/')
        self.mgr.disconnect(sid, '/')
        auth = {'recovery': {'token': creds['token'], 'offset': 0}}

        _, r1 = self.connect('eio2', auth=auth)
        _, r2 = self.connect('eio3', auth=auth)
        assert r1['recovered'] is True
        assert r2['recovered'] is False
        assert r2['reason'] == 'invalid'

    def test_concurrent_recovery_first_one_wins(self):
        sid, creds = self.connect('eio1')
        self.mgr.emit('news', 'a', '/')
        self.mgr.disconnect(sid, '/')
        auth = {'recovery': {'token': creds['token'], 'offset': 0}}
        sid2 = self.mgr.connect('eio2', '/')
        sid3 = self.mgr.connect('eio3', '/')

        results = []
        barrier = threading.Barrier(2)

        def recover(s):
            barrier.wait()
            results.append(self.mgr.recover_session(s, '/', auth))

        threads = [threading.Thread(target=recover, args=(s,))
                   for s in (sid2, sid3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(r['recovered'] for r in results) == [False, True]
        loser = [r for r in results if not r['recovered']][0]
        assert loser['reason'] == 'invalid'

    def test_purge_expired_sessions(self):
        sid, creds = self.connect('eio1')
        self.mgr.disconnect(sid, '/')
        assert len(self.mgr._recovery_sessions) == 1
        self.clock.advance(31)
        self.mgr.purge_expired_sessions()
        assert self.mgr._recovery_sessions == {}
        assert self.mgr._recovery_by_sid == {}

    def test_default_manager_has_no_recovery(self):
        assert not getattr(manager.Manager(), 'recovery_enabled', False)


@mock.patch('socketio.server.engineio.Server', **{
    'return_value.generate_id.side_effect': [str(i) for i in range(1, 20)]})
class TestRecoveryServer:
    def make_server(self, **kwargs):
        clock = FakeClock()
        mgr = recovery_manager.RecoveryManager(
            recovery_timeout=30, buffer_max_events=3, buffer_max_age=10,
            clock=clock, **kwargs)
        s = server.Server(client_manager=mgr, async_handlers=False)
        s.on('connect', mock.MagicMock())
        return s, clock

    def connect(self, s, eio_sid, auth=None):
        s._handle_eio_connect(eio_sid, 'environ')
        if auth is None:
            s._handle_eio_message(eio_sid, '0')
        else:
            s._handle_eio_message(eio_sid, '0' + json.dumps(auth))
        pkt = s.eio.send.call_args[0][1]
        assert pkt[0] == '0'
        return json.loads(pkt[1:])

    def test_connect_includes_recovery_credentials(self, eio):
        s, _ = self.make_server()
        data = self.connect(s, '123')
        assert data['sid'] == '1'
        assert data['recovery']['recovered'] is False
        assert data['recovery']['offset'] == 0
        assert data['recovery']['token']
        assert 'reason' not in data['recovery']

    def test_recovery_flow(self, eio):
        s, _ = self.make_server()
        data = self.connect(s, '123')
        token = data['recovery']['token']
        s.emit('news', 'a')
        s.emit('news', 'b')
        s._handle_disconnect('123', '/')
        s.emit('news', 'c')
        s.eio.send_packet.reset_mock()

        data = self.connect(s, '456', auth={'recovery': {
            'token': token, 'offset': 1}})
        assert data['recovery']['recovered'] is True
        assert data['recovery']['offset'] == 3
        assert data['recovery']['token'] != token
        # the missed events are replayed after the CONNECT packet
        replayed = [decode(c[0][1]) for c in
                    s.eio.send_packet.call_args_list]
        assert replayed == [['news', 'b'], ['news', 'c']]

    def test_recovery_gap_is_reported(self, eio):
        s, _ = self.make_server()
        data = self.connect(s, '123')
        token = data['recovery']['token']
        for i in range(5):  # buffer_max_events is 3
            s.emit('news', i)
        s._handle_disconnect('123', '/')
        s.eio.send_packet.reset_mock()

        data = self.connect(s, '456', auth={'recovery': {
            'token': token, 'offset': 0}})
        assert data['recovery']['recovered'] is False
        assert data['recovery']['reason'] == 'gap'
        assert s.eio.send_packet.call_count == 0

    def test_default_server_ignores_recovery(self, eio):
        s = server.Server(async_handlers=False)
        s.on('connect', mock.MagicMock())
        s._handle_eio_connect('123', 'environ')
        s._handle_eio_message(
            '123', '0{"recovery":{"token":"x","offset":0}}')
        # without a recovery manager the CONNECT packet is unchanged
        s.eio.send.assert_called_once_with('123', '0{"sid":"1"}')

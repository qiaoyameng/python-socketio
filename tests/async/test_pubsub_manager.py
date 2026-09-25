import asyncio
import functools
import json
from unittest import mock

import pytest

from engineio.packet import Packet as EIOPacket
from socketio import base_manager
from socketio import async_manager
from socketio import async_pubsub_manager
from socketio import async_server
from socketio import packet


class TestAsyncPubSubManager:
    def setup_method(self):
        id = 0

        def generate_id():
            nonlocal id
            id += 1
            return str(id)

        mock_server = mock.MagicMock()
        mock_server.eio.generate_id = generate_id
        mock_server.packet_class = packet.Packet
        mock_server._send_packet = mock.AsyncMock()
        mock_server._send_eio_packet = mock.AsyncMock()
        mock_server.disconnect = mock.AsyncMock()
        self.pm = async_pubsub_manager.AsyncPubSubManager()
        self.pm._publish = mock.AsyncMock()
        self.pm.set_server(mock_server)
        self.pm.host_id = '123456'
        self.pm.initialize()

    async def test_default_init(self):
        assert self.pm.channel == 'socketio'
        self.pm.server.start_background_task.assert_called_once_with(
            self.pm._thread
        )

    async def test_custom_init(self):
        pubsub = async_pubsub_manager.AsyncPubSubManager(channel='foo')
        assert pubsub.channel == 'foo'
        assert len(pubsub.host_id) == 32

    async def test_write_only_init(self):
        mock_server = mock.MagicMock()
        pm = async_pubsub_manager.AsyncPubSubManager(write_only=True)
        pm.set_server(mock_server)
        pm.initialize()
        assert pm.channel == 'socketio'
        assert len(pm.host_id) == 32
        assert pm.server.start_background_task.call_count == 0

    async def test_emit(self):
        await self.pm.emit('foo', 'bar')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_binary(self):
        await self.pm.emit('foo', b'bar')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [[{'_placeholder': True, 'num': 0}], 'YmFy'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )
        await self.pm.emit('foo', {'foo': b'bar'})
        self.pm._publish.assert_awaited_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [[{'foo': {'_placeholder': True, 'num': 0}}], 'YmFy'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_bytearray(self):
        await self.pm.emit('foo', bytearray(b'bar'))
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [[{'_placeholder': True, 'num': 0}], 'YmFy'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )
        await self.pm.emit('foo', {'foo': bytearray(b'bar')})
        self.pm._publish.assert_awaited_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [[{'foo': {'_placeholder': True, 'num': 0}}], 'YmFy'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_list(self):
        await self.pm.emit('foo', [1, 'two'])
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': [[1, 'two']],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )
        await self.pm.emit('foo', [1, b'two', 'three'])
        self.pm._publish.assert_awaited_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [
                    [[1, {'_placeholder': True, 'num': 0}, 'three']], 'dHdv',
                ],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_no_arguments(self):
        await self.pm.emit('foo', ())
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': [],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_multiple_arguments(self):
        await self.pm.emit('foo', (1, 'two'))
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': [1, 'two'],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )
        await self.pm.emit('foo', (1, b'two', 'three'))
        self.pm._publish.assert_awaited_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': True,
                'data': [
                    [1, {'_placeholder': True, 'num': 0}, 'three'], 'dHdv',
                ],
                'namespace': '/',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_to(self):
        sid = 'room-mate'
        await self.pm.emit('foo', 'bar', to=sid)
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/',
                'room': sid,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_namespace(self):
        await self.pm.emit('foo', 'bar', namespace='/baz')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/baz',
                'room': None,
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_room(self):
        await self.pm.emit('foo', 'bar', room='baz')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/',
                'room': 'baz',
                'skip_sid': None,
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_skip_sid(self):
        await self.pm.emit('foo', 'bar', skip_sid='baz')
        self.pm._publish.assert_awaited_once_with(
            {
                'method': 'emit',
                'event': 'foo',
                'binary': False,
                'data': ['bar'],
                'namespace': '/',
                'room': None,
                'skip_sid': 'baz',
                'callback': None,
                'host_id': '123456',
            }
        )

    async def test_emit_with_callback(self):
        with mock.patch.object(
            self.pm, '_generate_ack_id', return_value='123'
        ):
            await self.pm.emit('foo', 'bar', room='baz', callback='cb')
            self.pm._publish.assert_awaited_once_with(
                {
                    'method': 'emit',
                    'event': 'foo',
                    'binary': False,
                    'data': ['bar'],
                    'namespace': '/',
                    'room': 'baz',
                    'skip_sid': None,
                    'callback': ('baz', '/', '123'),
                    'host_id': '123456',
                }
            )

    async def test_emit_with_callback_without_server(self):
        standalone_pm = async_pubsub_manager.AsyncPubSubManager()
        with pytest.raises(RuntimeError):
            await standalone_pm.emit('foo', 'bar', callback='cb')

    async def test_emit_with_callback_missing_room(self):
        with mock.patch.object(
            self.pm, '_generate_ack_id', return_value='123'
        ):
            with pytest.raises(ValueError):
                await self.pm.emit('foo', 'bar', callback='cb')

    async def test_emit_with_ignore_queue(self):
        sid = await self.pm.connect('123', '/')
        await self.pm.emit(
            'foo', 'bar', room=sid, namespace='/', ignore_queue=True
        )
        self.pm._publish.assert_not_awaited()
        assert self.pm.server._send_eio_packet.await_count == 1
        assert self.pm.server._send_eio_packet.await_args_list[0][0][0] \
            == '123'
        pkt = self.pm.server._send_eio_packet.await_args_list[0][0][1]
        assert pkt.encode() == '42["foo","bar"]'

    async def test_can_disconnect(self):
        sid = await self.pm.connect('123', '/')
        assert await self.pm.can_disconnect(sid, '/') is True
        await self.pm.can_disconnect(sid, '/foo')
        self.pm._publish.assert_awaited_once_with(
            {'method': 'disconnect', 'sid': sid, 'namespace': '/foo',
             'host_id': '123456'}
        )

    async def test_disconnect(self):
        await self.pm.disconnect('foo', '/')
        self.pm._publish.assert_awaited_once_with(
            {'method': 'disconnect', 'sid': 'foo', 'namespace': '/',
             'host_id': '123456'}
        )

    async def test_disconnect_ignore_queue(self):
        sid = await self.pm.connect('123', '/')
        self.pm.pre_disconnect(sid, '/')
        await self.pm.disconnect(sid, '/', ignore_queue=True)
        self.pm._publish.assert_not_awaited()
        assert self.pm.is_connected(sid, '/') is False

    async def test_enter_room(self):
        sid = await self.pm.connect('123', '/')
        await self.pm.enter_room(sid, '/', 'foo')
        await self.pm.enter_room('456', '/', 'foo')
        assert sid in self.pm.rooms['/']['foo']
        assert self.pm.rooms['/']['foo'][sid] == '123'
        self.pm._publish.assert_awaited_once_with(
            {'method': 'enter_room', 'sid': '456', 'room': 'foo',
             'namespace': '/', 'host_id': '123456'}
        )

    async def test_leave_room(self):
        sid = await self.pm.connect('123', '/')
        await self.pm.leave_room(sid, '/', 'foo')
        await self.pm.leave_room('456', '/', 'foo')
        assert 'foo' not in self.pm.rooms['/']
        self.pm._publish.assert_awaited_once_with(
            {'method': 'leave_room', 'sid': '456', 'room': 'foo',
             'namespace': '/', 'host_id': '123456'}
        )

    async def test_close_room(self):
        await self.pm.close_room('foo')
        self.pm._publish.assert_awaited_once_with(
            {'method': 'close_room', 'room': 'foo', 'namespace': '/',
             'host_id': '123456'}
        )

    async def test_close_room_with_namespace(self):
        await self.pm.close_room('foo', '/bar')
        self.pm._publish.assert_awaited_once_with(
            {'method': 'close_room', 'room': 'foo', 'namespace': '/bar',
             'host_id': '123456'}
        )

    async def test_handle_emit(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': ['bar']})
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_legacy_emit(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': 'bar'})
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_binary(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [[{'_placeholder': True, 'num': 0}], 'YmFy'],
            })
            super_emit.assert_awaited_once_with(
                'foo',
                b'bar',
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [[{'foo': {'_placeholder': True, 'num': 0}}], 'YmFy'],
            })
            super_emit.assert_awaited_with(
                'foo',
                {'foo': b'bar'},
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_legacy_emit_binary(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [{'_placeholder': True, 'num': 0}, 'YmFy'],
            })
            super_emit.assert_awaited_once_with(
                'foo',
                b'bar',
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [{'foo': {'_placeholder': True, 'num': 0}}, 'YmFy'],
            })
            super_emit.assert_awaited_with(
                'foo',
                {'foo': b'bar'},
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_list(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': [[1, 'two']]})
            super_emit.assert_awaited_once_with(
                'foo',
                [1, 'two'],
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [
                    [[1, {'_placeholder': True, 'num': 0}, 'three']], 'dHdv'
                ]
            })
            super_emit.assert_awaited_with(
                'foo',
                [1, b'two', 'three'],
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_no_arguments(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': []})
            super_emit.assert_awaited_once_with(
                'foo',
                (),
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_multiple_arguments(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit({'event': 'foo', 'data': [1, 'two']})
            super_emit.assert_awaited_once_with(
                'foo',
                (1, 'two'),
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )
            await self.pm._handle_emit({
                'event': 'foo',
                'binary': True,
                'data': [
                    [1, {'_placeholder': True, 'num': 0}, 'three'], 'dHdv'
                ]
            })
            super_emit.assert_awaited_with(
                'foo',
                (1, b'two', 'three'),
                namespace=None,
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_with_namespace(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {'event': 'foo', 'data': 'bar', 'namespace': '/baz'}
            )
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace='/baz',
                room=None,
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_with_room(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {'event': 'foo', 'data': 'bar', 'room': 'baz'}
            )
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace=None,
                room='baz',
                skip_sid=None,
                callback=None,
            )

    async def test_handle_emit_with_skip_sid(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {'event': 'foo', 'data': 'bar', 'skip_sid': '123'}
            )
            super_emit.assert_awaited_once_with(
                'foo',
                'bar',
                namespace=None,
                room=None,
                skip_sid='123',
                callback=None,
            )

    async def test_handle_emit_with_remote_callback(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {
                    'event': 'foo',
                    'data': 'bar',
                    'namespace': '/baz',
                    'callback': ('sid', '/baz', 123),
                    'host_id': 'x',
                }
            )
            assert super_emit.await_count == 1
            assert super_emit.await_args[0] == ('foo', 'bar')
            assert super_emit.await_args[1]['namespace'] == '/baz'
            assert super_emit.await_args[1]['room'] is None
            assert super_emit.await_args[1]['skip_sid'] is None
            assert isinstance(
                super_emit.await_args[1]['callback'], functools.partial
            )
            await super_emit.await_args[1]['callback']('one', 2, 'three')
            self.pm._publish.assert_awaited_once_with(
                {
                    'method': 'callback',
                    'host_id': 'x',
                    'sid': 'sid',
                    'namespace': '/baz',
                    'id': 123,
                    'args': ('one', 2, 'three'),
                }
            )

    async def test_handle_emit_with_local_callback(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'emit'
        ) as super_emit:
            await self.pm._handle_emit(
                {
                    'event': 'foo',
                    'data': 'bar',
                    'namespace': '/baz',
                    'callback': ('sid', '/baz', 123),
                    'host_id': self.pm.host_id,
                }
            )
            assert super_emit.await_count == 1
            assert super_emit.await_args[0] == ('foo', 'bar')
            assert super_emit.await_args[1]['namespace'] == '/baz'
            assert super_emit.await_args[1]['room'] is None
            assert super_emit.await_args[1]['skip_sid'] is None
            assert isinstance(
                super_emit.await_args[1]['callback'], functools.partial
            )
            await super_emit.await_args[1]['callback']('one', 2, 'three')
            self.pm._publish.assert_not_awaited()

    async def test_handle_callback(self):
        host_id = self.pm.host_id
        with mock.patch.object(
            self.pm, 'trigger_callback'
        ) as trigger:
            await self.pm._handle_callback(
                {
                    'method': 'callback',
                    'host_id': host_id,
                    'sid': 'sid',
                    'namespace': '/',
                    'id': 123,
                    'args': ('one', 2),
                }
            )
            trigger.assert_awaited_once_with('sid', 123, ('one', 2))

    async def test_handle_callback_bad_host_id(self):
        with mock.patch.object(
            self.pm, 'trigger_callback'
        ) as trigger:
            await self.pm._handle_callback(
                {
                    'method': 'callback',
                    'host_id': 'bad',
                    'sid': 'sid',
                    'namespace': '/',
                    'id': 123,
                    'args': ('one', 2),
                }
            )
            assert trigger.await_count == 0

    async def test_handle_callback_missing_args(self):
        host_id = self.pm.host_id
        with mock.patch.object(
            self.pm, 'trigger_callback'
        ) as trigger:
            await self.pm._handle_callback(
                {
                    'method': 'callback',
                    'host_id': host_id,
                    'sid': 'sid',
                    'namespace': '/',
                    'id': 123,
                }
            )
            await self.pm._handle_callback(
                {
                    'method': 'callback',
                    'host_id': host_id,
                    'sid': 'sid',
                    'namespace': '/',
                }
            )
            await self.pm._handle_callback(
                {'method': 'callback', 'host_id': host_id, 'sid': 'sid'}
            )
            await self.pm._handle_callback(
                {'method': 'callback', 'host_id': host_id}
            )
            assert trigger.await_count == 0

    async def test_handle_disconnect(self):
        await self.pm._handle_disconnect(
            {'method': 'disconnect', 'sid': '123', 'namespace': '/foo'}
        )
        self.pm.server.disconnect.assert_awaited_once_with(
            sid='123', namespace='/foo', ignore_queue=True
        )

    async def test_handle_enter_room(self):
        sid = await self.pm.connect('123', '/')
        with mock.patch.object(
            async_manager.AsyncManager, 'enter_room'
        ) as super_enter_room:
            await self.pm._handle_enter_room(
                {'method': 'enter_room', 'sid': sid, 'namespace': '/',
                 'room': 'foo'}
            )
            await self.pm._handle_enter_room(
                {'method': 'enter_room', 'sid': '456', 'namespace': '/',
                 'room': 'foo'}
            )
            super_enter_room.assert_awaited_once_with(sid, '/', 'foo')

    async def test_handle_leave_room(self):
        sid = await self.pm.connect('123', '/')
        with mock.patch.object(
            async_manager.AsyncManager, 'leave_room'
        ) as super_leave_room:
            await self.pm._handle_leave_room(
                {'method': 'leave_room', 'sid': sid, 'namespace': '/',
                 'room': 'foo'}
            )
            await self.pm._handle_leave_room(
                {'method': 'leave_room', 'sid': '456', 'namespace': '/',
                 'room': 'foo'}
            )
            super_leave_room.assert_awaited_once_with(sid, '/', 'foo')

    async def test_handle_close_room(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'close_room'
        ) as super_close_room:
            await self.pm._handle_close_room(
                {'method': 'close_room', 'room': 'foo'}
            )
            super_close_room.assert_awaited_once_with(
                room='foo', namespace=None
            )

    async def test_handle_close_room_with_namespace(self):
        with mock.patch.object(
            async_manager.AsyncManager, 'close_room'
        ) as super_close_room:
            await self.pm._handle_close_room(
                {
                    'method': 'close_room',
                    'room': 'foo',
                    'namespace': '/bar',
                }
            )
            super_close_room.assert_awaited_once_with(
                room='foo', namespace='/bar'
            )

    async def test_background_thread(self):
        self.pm._handle_emit = mock.AsyncMock()
        self.pm._handle_callback = mock.AsyncMock()
        self.pm._handle_disconnect = mock.AsyncMock()
        self.pm._handle_enter_room = mock.AsyncMock()
        self.pm._handle_leave_room = mock.AsyncMock()
        self.pm._handle_close_room = mock.AsyncMock()
        host_id = self.pm.host_id

        async def messages():
            yield {'method': 'emit', 'value': 'foo', 'host_id': 'x'}
            yield {'missing': 'method', 'host_id': 'x'}
            yield '{"method": "callback", "value": "bar", "host_id": "x"}'
            yield {'method': 'disconnect', 'sid': '123', 'namespace': '/foo',
                   'host_id': 'x'}
            yield {'method': 'bogus', 'host_id': 'x'}
            yield json.dumps({'method': 'close_room', 'value': 'baz',
                              'host_id': 'x'})
            yield {'method': 'enter_room', 'sid': '123', 'namespace': '/foo',
                   'room': 'room', 'host_id': 'x'}
            yield {'method': 'leave_room', 'sid': '123', 'namespace': '/foo',
                   'room': 'room', 'host_id': 'x'}
            yield 'bad json'
            yield b'bad data'

            # these should not publish anything on the queue, as they come from
            # the same host
            yield {'method': 'emit', 'value': 'foo', 'host_id': host_id}
            yield {'method': 'callback', 'value': 'bar', 'host_id': host_id}
            yield {'method': 'disconnect', 'sid': '123', 'namespace': '/foo',
                   'host_id': host_id}
            yield json.dumps({'method': 'close_room', 'value': 'baz',
                              'host_id': host_id})

        self.pm._listen = messages
        await self.pm._thread()

        self.pm._handle_emit.assert_awaited_once_with(
            {'method': 'emit', 'value': 'foo', 'host_id': 'x'}
        )
        self.pm._handle_callback.assert_any_await(
            {'method': 'callback', 'value': 'bar', 'host_id': 'x'}
        )
        self.pm._handle_callback.assert_any_await(
            {'method': 'callback', 'value': 'bar', 'host_id': host_id}
        )
        self.pm._handle_disconnect.assert_awaited_once_with(
            {'method': 'disconnect', 'sid': '123', 'namespace': '/foo',
             'host_id': 'x'}
        )
        self.pm._handle_enter_room.assert_awaited_once_with(
            {'method': 'enter_room', 'sid': '123', 'namespace': '/foo',
             'room': 'room', 'host_id': 'x'}
        )
        self.pm._handle_leave_room.assert_awaited_once_with(
            {'method': 'leave_room', 'sid': '123', 'namespace': '/foo',
             'room': 'room', 'host_id': 'x'}
        )
        self.pm._handle_close_room.assert_awaited_once_with(
            {'method': 'close_room', 'value': 'baz', 'host_id': 'x'}
        )

    async def test_background_thread_exception(self):
        self.pm._handle_emit = mock.AsyncMock(side_effect=[
            ValueError(), asyncio.CancelledError])

        async def messages():
            yield {'method': 'emit', 'value': 'foo', 'host_id': 'x'}
            yield {'method': 'emit', 'value': 'bar', 'host_id': 'x'}

        self.pm._listen = messages
        await self.pm._thread()

        self.pm._handle_emit.assert_any_await(
            {'method': 'emit', 'value': 'foo', 'host_id': 'x'}
        )
        self.pm._handle_emit.assert_awaited_with(
            {'method': 'emit', 'value': 'bar', 'host_id': 'x'}
        )

    def test_custom_json(self):
        saved_json = packet.Packet.json

        cm = async_pubsub_manager.AsyncPubSubManager(json='foo')
        assert cm.json == 'foo'
        async_server.AsyncServer(json='bar', client_manager=cm)
        assert cm.json == 'bar'

        packet.Packet.json = saved_json
        EIOPacket.json = saved_json


class FakeChannel:
    """A fake pub/sub channel that connects any number of managers.

    Messages are serialized and deserialized with JSON to simulate what a
    real pub/sub backend would do with them.
    """
    def __init__(self):
        self.managers = []
        self.messages = []

    def attach(self, pm):
        self.managers.append(pm)
        pm._publish = functools.partial(self.publish, pm)

    async def publish(self, sender, message):
        message = json.loads(json.dumps(message))
        self.messages.append(message)
        for pm in self.managers:
            if pm is not sender:
                if message['method'] == 'emit':
                    await pm._handle_emit(message)


class TestAsyncPubSubRoomSelection:
    def setup_method(self):
        def make_manager(prefix):
            id = 0

            def generate_id():
                nonlocal id
                id += 1
                return '{}{}'.format(prefix, id)

            mock_server = mock.MagicMock()
            mock_server.eio.generate_id = generate_id
            mock_server.packet_class = packet.Packet
            mock_server._send_packet = mock.AsyncMock()
            mock_server._send_eio_packet = mock.AsyncMock()
            pm = async_pubsub_manager.AsyncPubSubManager()
            pm.set_server(mock_server)
            pm.initialize()
            return pm

        self.channel = FakeChannel()
        self.pm1 = make_manager('a')
        self.pm2 = make_manager('b')
        self.channel.attach(self.pm1)
        self.channel.attach(self.pm2)

    async def connect(self, pm, eio_sid, rooms=()):
        sid = await pm.connect(eio_sid, '/')
        for room in rooms:
            await pm.enter_room(sid, '/', room)
        return sid

    async def test_selection_cross_node_overlap(self):
        # node 1: a1 is in project and on duty, a2 only in project
        await self.connect(self.pm1, 'eio-a1', ['project', 'on-duty'])
        await self.connect(self.pm1, 'eio-a2', ['project'])
        # node 2: b1 is also muted, b2 is in project and on duty
        await self.connect(self.pm2, 'eio-b1', ['project', 'on-duty',
                                                'muted'])
        await self.connect(self.pm2, 'eio-b2', ['project', 'on-duty'])

        await self.pm1.emit('notice', 'hello',
                            to=base_manager.RoomSelection(
                                union='project', intersect='on-duty',
                                exclude='muted'))

        # the selection criteria, and not a list of sids, must have been
        # sent through the channel
        assert self.channel.messages[0]['room'] == {
            'union': ['project'], 'intersect': ['on-duty'],
            'exclude': ['muted']}

        # each node delivers to its own clients only, once per client
        assert self.pm1.server._send_eio_packet.await_count == 1
        assert self.pm1.server._send_eio_packet.await_args_list[0][0][0] \
            == 'eio-a1'
        assert self.pm2.server._send_eio_packet.await_count == 1
        assert self.pm2.server._send_eio_packet.await_args_list[0][0][0] \
            == 'eio-b2'

    async def test_selection_empty_intersection(self):
        await self.connect(self.pm1, 'eio-a1', ['project'])
        await self.connect(self.pm2, 'eio-b1', ['on-duty'])

        await self.pm1.emit('notice', 'hello',
                            to=base_manager.RoomSelection(
                                intersect=['project', 'on-duty']))

        assert self.pm1.server._send_eio_packet.await_count == 0
        assert self.pm2.server._send_eio_packet.await_count == 0

    async def test_selection_leave_room_during_send(self):
        await self.connect(self.pm1, 'eio-a1', ['project'])
        sid_b1 = await self.connect(self.pm2, 'eio-b1', ['project'])
        await self.connect(self.pm2, 'eio-b2', ['project'])

        async def leave_room(eio_sid, pkt):
            # a client leaving the room on the remote node while the event
            # is being sent must not break the delivery
            await self.pm2.leave_room(sid_b1, '/', 'project')

        self.pm2.server._send_eio_packet.side_effect = leave_room
        await self.pm1.emit('notice', 'hello',
                            to=base_manager.RoomSelection(union='project'))

        assert self.pm1.server._send_eio_packet.await_count == 1
        assert self.pm2.server._send_eio_packet.await_count == 2
        recipients = [c[0][0] for c in
                      self.pm2.server._send_eio_packet.await_args_list]
        assert recipients == ['eio-b1', 'eio-b2']

    async def test_selection_binary_event(self):
        await self.connect(self.pm1, 'eio-a1', ['project'])
        await self.connect(self.pm2, 'eio-b1', ['project'])

        await self.pm1.emit('notice', b'\x00\x01',
                            to=base_manager.RoomSelection(union='project'))

        assert self.channel.messages[0]['binary'] is True
        for pm in [self.pm1, self.pm2]:
            assert pm.server._send_eio_packet.await_count == 2
            pkt = pm.server._send_eio_packet.await_args_list[1][0][1]
            assert pkt.encode() == b'\x00\x01'

    async def test_selection_with_callback_rejected(self):
        await self.connect(self.pm1, 'eio-a1', ['project'])
        with pytest.raises(ValueError):
            await self.pm1.emit(
                'notice', 'hello',
                to=base_manager.RoomSelection(union='project'),
                callback=lambda: None)
        assert self.channel.messages == []
        assert self.pm1.server._send_eio_packet.await_count == 0

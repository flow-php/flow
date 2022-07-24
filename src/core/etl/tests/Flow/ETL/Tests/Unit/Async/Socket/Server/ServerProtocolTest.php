<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Async\Socket\Server;

use Flow\ETL\Async\Socket\Communication\Message;
use Flow\ETL\Async\Socket\Server\Client;
use Flow\ETL\Async\Socket\Server\Server;
use Flow\ETL\Async\Socket\Server\ServerProtocol;
use Flow\ETL\Async\Socket\Worker\Pool;
use Flow\ETL\Cache\InMemoryCache;
use Flow\ETL\Config;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ServerProtocolTest extends TestCase
{
    public function test_disconnecting_client_after_unsuccessful_identification() : void
    {
        $serverProtocol = new ServerProtocol(
            new FlowContext(Config::builder()->cache(new InMemoryCache())->build()),
            'cache_id',
            $pool = Pool::generate(1),
            (new ProcessExtractor(new Rows())),
            $pipes = Pipes::empty()
        );

        $client = $this->createMock(Client::class);
        $server = $this->createMock(Server::class);

        $client->expects($this->once())
            ->method('disconnect');

        $serverProtocol->handle(
            Message::identify('invalid-id'),
            $client,
            $server
        );
    }

    public function test_send_rows_after_fetch_when_there_are_still_some_rows_left_otherwise_stop_server() : void
    {
        $serverProtocol = new ServerProtocol(
            new Flowcontext(Config::builder()->cache(new InMemoryCache())->build()),
            'cache_id',
            $pool = Pool::generate(1),
            new ProcessExtractor($rows = new Rows()),
            Pipes::empty()
        );

        $client = $this->createMock(Client::class);
        $server = $this->createMock(Server::class);

        $client->expects($this->once())
            ->method('send')
            ->with(Message::process($rows, false));

        $server->expects($this->exactly(2))
            ->method('isRunning')
            ->willReturnOnConsecutiveCalls(
                true,
                false
            );

        $server->expects($this->once())
            ->method('stop');

        $serverProtocol->handle(
            Message::fetch($pool->ids()[0]),
            $client,
            $server
        );
        $serverProtocol->handle(
            Message::fetch($pool->ids()[0]),
            $client,
            $server
        );
    }

    public function test_sending_pipes_after_successful_identification() : void
    {
        $serverProtocol = new ServerProtocol(
            new FlowContext(Config::builder()->cache($cache = new InMemoryCache())->build()),
            'cache_id',
            $pool = Pool::generate(1),
            (new ProcessExtractor(new Rows())),
            $pipes = Pipes::empty()
        );

        $client = $this->createMock(Client::class);
        $server = $this->createMock(Server::class);

        $client->expects($this->once())
            ->method('send')
            ->with(Message::setup($pipes, $cache, 'cache_id'));

        $serverProtocol->handle(
            Message::identify(\current($pool->ids())),
            $client,
            $server
        );
    }

    public function test_stop_server_when_last_client_is_disconnected() : void
    {
        $serverProtocol = new ServerProtocol(
            new FlowContext(Config::builder()->cache($cache = new InMemoryCache())->build()),
            'cache_id',
            $pool = Pool::generate(2),
            new ProcessExtractor(),
            $pipes = Pipes::empty()
        );

        $client1 = $this->createMock(Client::class);
        $client2 = $this->createMock(Client::class);
        $server = $this->createMock(Server::class);

        // send to client setup message after successful identification
        $client1->expects($this->once())
            ->method('send')
            ->with(Message::setup($pipes, $cache, 'cache_id'));
        $client2->expects($this->once())
            ->method('send')
            ->with(Message::setup($pipes, $cache, 'cache_id'));

        // disconnect clients after attempt to fetch rows when there are no rows left
        $client1->expects($this->once())
            ->method('disconnect');
        $client2->expects($this->once())
            ->method('disconnect');

        // when there are no connected clients and there are no rows left check if server is running and stop it
        $server->expects($this->once())
            ->method('isRunning')
            ->willReturn(true);
        $server->expects($this->once())
            ->method('stop');

        // identify both clients
        $serverProtocol->handle(Message::identify($pool->ids()[0]), $client1, $server);
        $serverProtocol->handle(Message::identify($pool->ids()[1]), $client2, $server);

        $this->assertCount(2, $pool->onlyConnected());

        // there are no rows left, disconnect on fetch attempt
        $serverProtocol->handle(Message::fetch($pool->ids()[0]), $client1, $server);
        $serverProtocol->handle(Message::fetch($pool->ids()[1]), $client2, $server);

        $this->assertCount(0, $pool->onlyConnected());
    }
}

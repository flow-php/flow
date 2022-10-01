<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Async\Socket\Worker;

use Flow\ETL\Async\Socket\Communication\Message;
use Flow\ETL\Async\Socket\Worker\ClientProtocol;
use Flow\ETL\Async\Socket\Worker\Processor;
use Flow\ETL\Async\Socket\Worker\Server;
use Flow\ETL\Cache\InMemoryCache;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class ClientProtocolTest extends TestCase
{
    public function test_cache_rows_and_fetch_next_after_getting_processing_rows() : void
    {
        $protocol = new ClientProtocol(new Processor('worker', new NullLogger()));

        $server = $this->createMock(Server::class);

        $server->expects($this->exactly(2))
            ->method('send')
            ->with(Message::fetch('id'));

        $context = new FlowContext(Config::builder()->cache($cache = new InMemoryCache())->build());

        $protocol->handle('id', Message::setup(Pipes::empty(), $context, 'cache_id'), $server);
        $protocol->handle(
            'id',
            Message::process($rows = new Rows(Row::create(new Row\Entry\IntegerEntry('id', 1)))),
            $server
        );

        $this->assertEquals(
            [$rows],
            \iterator_to_array($cache->read('cache_id'))
        );
    }

    public function test_fetch_after_getting_pipes() : void
    {
        $protocol = new ClientProtocol(new Processor('worker', new NullLogger()));

        $context = new FlowContext(Config::builder()->cache(new InMemoryCache())->build());

        $server = $this->createMock(Server::class);

        $server->expects($this->once())
            ->method('send')
            ->with(Message::fetch('id'));

        $protocol->handle(
            'id',
            Message::setup(Pipes::empty(), $context, \uniqid()),
            $server
        );
    }
}

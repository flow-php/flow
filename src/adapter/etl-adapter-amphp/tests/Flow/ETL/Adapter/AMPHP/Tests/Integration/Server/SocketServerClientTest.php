<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\AMPHP\Tests\Integration\Server;

use const Amp\Process\IS_WINDOWS;
use function Amp\async;
use function Flow\ETL\DSL\lit;
use Flow\ETL\Async\Amp\Server\SocketServer;
use Flow\ETL\Async\Amp\Worker\SocketClient;
use Flow\ETL\Async\Socket\Server\ServerProtocol;
use Flow\ETL\Async\Socket\Worker\ClientProtocol;
use Flow\ETL\Async\Socket\Worker\Pool;
use Flow\ETL\Async\Socket\Worker\Processor;
use Flow\ETL\Cache\LocalFilesystemCache;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\EntryExpressionEvalTransformer;
use Flow\Serializer\NativePHPSerializer;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class SocketServerClientTest extends TestCase
{
    public function test_start_tcp_server() : void
    {
        $server = SocketServer::tcp(6541, $logger = new NullLogger());
        $cache = new LocalFilesystemCache(\sys_get_temp_dir(), new NativePHPSerializer());
        $cacheId = \uniqid('cache_id');

        $server->initialize(new ServerProtocol(
            new FlowContext(Config::builder()->cache($cache)->build()),
            $cacheId,
            $pool = Pool::generate(1),
            new ProcessExtractor(
                $rows = new Rows(Row::create(Entry::integer('id', 1)))
            ),
            new Pipes([new EntryExpressionEvalTransformer('active', lit(true))])
        ));

        async(function () use ($logger, $pool) : void {
            $client = new SocketClient($logger);
            $client->connect($pool->ids()[0], '127.0.0.1:6541', new ClientProtocol(new Processor($pool->ids()[0], $logger)));
        });

        $server->start();

        $this->assertFalse($server->isRunning());

        $this->assertEquals(
            [$rows->map(fn (Row $row) => $row->add(Entry::boolean('active', true)))],
            \iterator_to_array($cache->read($cacheId))
        );

        $cache->clear($cacheId);
    }

    public function test_start_unix_domain_socket_server() : void
    {
        if (IS_WINDOWS) {
            $this->markTestSkipped();
        }

        $server = SocketServer::unixDomain(\sys_get_temp_dir(), $logger = new NullLogger());
        $cache = new LocalFilesystemCache(\sys_get_temp_dir(), new NativePHPSerializer());
        $cacheId = \uniqid('cache_id');

        $server->initialize(new ServerProtocol(
            new FlowContext(Config::builder()->cache($cache)->build()),
            $cacheId,
            $pool = Pool::generate(1),
            new ProcessExtractor(
                $rows = new Rows(Row::create(Entry::integer('id', 1)))
            ),
            new Pipes([new EntryExpressionEvalTransformer('active', lit(true))])
        ));

        async(function () use ($logger, $pool, $server) : void {
            $client = new SocketClient($logger);
            $client->connect($pool->ids()[0], $server->host(), new ClientProtocol(new Processor($pool->ids()[0], $logger)));
        });

        $server->start();

        $this->assertFalse($server->isRunning());

        $this->assertEquals(
            [$rows->map(fn (Row $row) => $row->add(Entry::boolean('active', true)))],
            \iterator_to_array($cache->read($cacheId))
        );

        $cache->clear($cacheId);

        $socketFile = \str_replace('unix://', '', $server->host());

        if (\file_exists($socketFile)) {
            @\unlink($socketFile);
        }
    }
}

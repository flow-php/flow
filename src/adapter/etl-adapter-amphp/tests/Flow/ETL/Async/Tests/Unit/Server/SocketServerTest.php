<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Tests\Unit\Server;

use Flow\ETL\Async\Amp\Server\SocketServer;
use Flow\ETL\Async\Socket\Server\ServerProtocol;
use Flow\ETL\Async\Socket\Worker\Pool;
use Flow\ETL\Config;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Pipes;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class SocketServerTest extends TestCase
{
    public function test_initialize_server_twice() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Server already initialized');

        $server = SocketServer::tcp(6541, $logger = new NullLogger());

        $server->initialize(new ServerProtocol(
            new FlowContext(Config::default()),
            'cache_id',
            Pool::generate(1),
            new ProcessExtractor(),
            Pipes::empty()
        ));

        $server->initialize(new ServerProtocol(
            new FlowContext(Config::default()),
            'cache_id',
            Pool::generate(1),
            new ProcessExtractor(),
            Pipes::empty()
        ));
    }

    public function test_start_server_without_initialiation() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Server not initialized');

        $server = SocketServer::tcp(6541, new NullLogger());

        $server->start();
    }

    public function test_unix_socket_using_non_exising_folder_path() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given path does not exists or is not valid folder: non_exisitng_folder');
        SocketServer::unixDomain('non_exisitng_folder', new NullLogger());
    }
}

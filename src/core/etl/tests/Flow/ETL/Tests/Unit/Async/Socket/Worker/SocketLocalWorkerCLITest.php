<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Async\Socket\Worker;

use Flow\ETL\Async\Socket\Worker\Client;
use Flow\ETL\Async\Socket\Worker\SocketLocalWorkerCLI;
use Flow\ETL\CLI\Input;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class SocketLocalWorkerCLITest extends TestCase
{
    public function test_running_cli_with_all_required_options() : void
    {
        $cli = new SocketLocalWorkerCLI(new NullLogger(), $this->createMock(Client::class));

        $this->assertSame(
            0,
            $cli->run(new Input(['bin/worker', '--host=127.0.0.1', '--port=6651', '--id="worker_id"']))
        );
    }

    public function test_running_cli_without_worker_id_option() : void
    {
        $cli = new SocketLocalWorkerCLI(new NullLogger(), $this->createMock(Client::class));

        $this->assertSame(
            1,
            $cli->run(new Input(['bin/worker', '--host=127.0.0.1', '--port=6651']))
        );
    }
}

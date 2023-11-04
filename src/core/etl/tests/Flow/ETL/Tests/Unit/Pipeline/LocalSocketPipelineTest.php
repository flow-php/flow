<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\Async\Socket\Server\Server;
use Flow\ETL\Async\Socket\Server\ServerProtocol;
use Flow\ETL\Async\Socket\Worker\Pool;
use Flow\ETL\Async\Socket\Worker\WorkerLauncher;
use Flow\ETL\Cache\InMemoryCache;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\LocalSocketPipeline;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CacheSpy;
use PHPUnit\Framework\TestCase;

final class LocalSocketPipelineTest extends TestCase
{
    public function test_creating_with_not_enough_workers() : void
    {
        $this->expectExceptionMessage("Number of workers can't be lower than 1, given: 0");
        $this->expectException(InvalidArgumentException::class);

        new LocalSocketPipeline(
            $this->createMock(Server::class),
            $this->createMock(WorkerLauncher::class),
            0
        );
    }

    public function test_has() : void
    {
        $pipeline = new LocalSocketPipeline(
            $this->createMock(Server::class),
            $this->createMock(WorkerLauncher::class),
            5
        );

        $this->assertFalse($pipeline->has('not-existing-class'));
    }

    public function test_pipeline() : void
    {
        $pipeline = new LocalSocketPipeline(
            $server = $this->createMock(Server::class),
            $launcher = $this->createMock(WorkerLauncher::class),
            5
        );

        $server->expects($this->once())
            ->method('initialize')
            ->with($this->isInstanceOf(ServerProtocol::class));

        $server->expects($this->once())
            ->method('start');

        $server->method('host')->willReturn('host');

        $launcher->expects($this->once())
            ->method('launch')
            ->with($this->isInstanceOf(Pool::class), 'host');

        $pipeline
            ->setSource(new ProcessExtractor(
                new Rows(
                    Row::create(Entry::integer('id', 1)),
                    Row::create(Entry::integer('id', 2)),
                    Row::create(Entry::integer('id', 3)),
                )
            ))
            ->process(new FlowContext(Config::builder()->cache($cache = new CacheSpy(new InMemoryCache()))->build()));

        $this->assertSame(1, $cache->reads());
    }
}

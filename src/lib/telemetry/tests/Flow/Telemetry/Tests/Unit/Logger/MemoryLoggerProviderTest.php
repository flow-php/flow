<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\{Logger, LoggerProvider, Severity};
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidLogExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\{ClockMother, ResourceMother};
use PHPUnit\Framework\TestCase;

final class MemoryLoggerProviderTest extends TestCase
{
    private Resource $resource;

    protected function setUp() : void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_creates_new_logger_each_time() : void
    {
        $provider = new LoggerProvider($this->createProcessor(), ClockMother::frozen(), new MemoryContextStorage());

        $logger1 = $provider->logger($this->resource, 'service-a', '1.0');
        $logger2 = $provider->logger($this->resource, 'service-a', '1.0');

        self::assertNotSame($logger1, $logger2);
    }

    public function test_logger_returns_logger_instance() : void
    {
        $provider = new LoggerProvider($this->createProcessor(), ClockMother::frozen(), new MemoryContextStorage());

        $logger = $provider->logger($this->resource, 'test', '1.0');

        self::assertInstanceOf(Logger::class, $logger);
    }

    public function test_processor_flush_returns_true() : void
    {
        $processor = $this->createProcessor();

        self::assertTrue($processor->flush());
    }

    public function test_processor_stores_logs_from_all_loggers() : void
    {
        $processor = $this->createProcessor();
        $provider = new LoggerProvider($processor, ClockMother::frozen(), new MemoryContextStorage());

        $logger1 = $provider->logger($this->resource, 'service-a', '1.0');
        $logger2 = $provider->logger($this->resource, 'service-b', '2.0');

        $logger1->info('message from service A');
        $logger2->error('message from service B');

        self::assertCount(2, $processor->entries());
        self::assertSame(Severity::INFO, $processor->entries()[0]->record->severity);
        self::assertSame(Severity::ERROR, $processor->entries()[1]->record->severity);
    }

    private function createProcessor() : MemoryLogProcessor
    {
        return new MemoryLogProcessor(new VoidLogExporter());
    }
}

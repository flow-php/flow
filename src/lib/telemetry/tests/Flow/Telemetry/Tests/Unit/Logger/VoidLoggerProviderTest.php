<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class VoidLoggerProviderTest extends TestCase
{
    public function test_logger_returns_logger(): void
    {
        $logger = (new LoggerProvider(
            new VoidLogProcessor(),
            ClockMother::frozen(),
            new MemoryContextStorage(),
        ))->logger(ResourceMother::default(), 'test', '1.0');

        static::assertInstanceOf(Logger::class, $logger);
    }

    public function test_processor_flush_returns_true(): void
    {
        $processor = new VoidLogProcessor();

        static::assertTrue($processor->flush());
    }
}

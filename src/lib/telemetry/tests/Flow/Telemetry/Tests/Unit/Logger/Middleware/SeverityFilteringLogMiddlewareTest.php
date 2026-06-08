<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Middleware;

use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\severity_filtering_log_middleware;

final class SeverityFilteringLogMiddlewareTest extends TestCase
{
    public function test_forwards_entry_at_minimum_severity(): void
    {
        $middleware = severity_filtering_log_middleware(Severity::WARN);
        $entry = LogEntryMother::create('warn', Severity::WARN);

        static::assertSame($entry, $middleware->process($entry));
    }

    public function test_forwards_entry_above_minimum_severity(): void
    {
        $middleware = severity_filtering_log_middleware(Severity::WARN);
        $entry = LogEntryMother::create('error', Severity::ERROR);

        static::assertSame($entry, $middleware->process($entry));
    }

    public function test_drops_entry_below_minimum_severity(): void
    {
        $middleware = severity_filtering_log_middleware(Severity::WARN);

        static::assertNull($middleware->process(LogEntryMother::create('info', Severity::INFO)));
    }

    #[DataProvider('belowFatalProvider')]
    public function test_only_fatal_passes_when_minimum_is_fatal(Severity $severity): void
    {
        $middleware = severity_filtering_log_middleware(Severity::FATAL);

        static::assertNull($middleware->process(LogEntryMother::create('msg', $severity)));
    }

    /**
     * @return iterable<string, array{Severity}>
     */
    public static function belowFatalProvider(): iterable
    {
        yield 'trace' => [Severity::TRACE];
        yield 'debug' => [Severity::DEBUG];
        yield 'info' => [Severity::INFO];
        yield 'warn' => [Severity::WARN];
        yield 'error' => [Severity::ERROR];
    }

    public function test_default_minimum_is_info(): void
    {
        $middleware = severity_filtering_log_middleware();

        static::assertNull($middleware->process(LogEntryMother::create('debug', Severity::DEBUG)));
        $info = LogEntryMother::create('info', Severity::INFO);
        static::assertSame($info, $middleware->process($info));
    }
}

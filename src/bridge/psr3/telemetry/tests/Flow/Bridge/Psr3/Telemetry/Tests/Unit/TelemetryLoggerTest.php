<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry\Tests\Unit;

use Flow\Bridge\Psr3\Telemetry\Exception\InvalidArgumentException;
use Flow\Bridge\Psr3\Telemetry\{LogRecordConverter, SeverityMapper, TelemetryLogger};
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\{LoggerProvider, Severity};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\LogLevel;

final class TelemetryLoggerTest extends TestCase
{
    private MemoryLogProcessor $processor;

    private TelemetryLogger $psrLogger;

    /**
     * @return \Generator<string, array{string, Severity}>
     */
    public static function level_to_severity_provider() : \Generator
    {
        yield 'debug' => [LogLevel::DEBUG, Severity::DEBUG];
        yield 'info' => [LogLevel::INFO, Severity::INFO];
        yield 'notice' => [LogLevel::NOTICE, Severity::INFO];
        yield 'warning' => [LogLevel::WARNING, Severity::WARN];
        yield 'error' => [LogLevel::ERROR, Severity::ERROR];
        yield 'critical' => [LogLevel::CRITICAL, Severity::FATAL];
        yield 'alert' => [LogLevel::ALERT, Severity::FATAL];
        yield 'emergency' => [LogLevel::EMERGENCY, Severity::FATAL];
    }

    protected function setUp() : void
    {
        $this->processor = new MemoryLogProcessor(new VoidExporter());

        $logger = (new LoggerProvider(
            $this->processor,
            new SystemClock(),
            new MemoryContextStorage(),
        ))->logger(Resource::empty(), 'psr3-test-scope');

        $this->psrLogger = new TelemetryLogger($logger);
    }

    public function test_alert_emits_fatal_severity() : void
    {
        $this->psrLogger->alert('alarm');

        self::assertSame(Severity::FATAL, $this->processor->entries()[0]->record->severity);
    }

    public function test_critical_emits_fatal_severity() : void
    {
        $this->psrLogger->critical('crit');

        self::assertSame(Severity::FATAL, $this->processor->entries()[0]->record->severity);
    }

    public function test_custom_converter_is_used() : void
    {
        $processor = new MemoryLogProcessor(new VoidExporter());
        $logger = (new LoggerProvider(
            $processor,
            new SystemClock(),
            new MemoryContextStorage(),
        ))->logger(Resource::empty(), 'psr3-custom-scope');

        $converter = new LogRecordConverter(new SeverityMapper([
            LogLevel::DEBUG => Severity::TRACE,
            LogLevel::INFO => Severity::INFO,
            LogLevel::NOTICE => Severity::INFO,
            LogLevel::WARNING => Severity::WARN,
            LogLevel::ERROR => Severity::ERROR,
            LogLevel::CRITICAL => Severity::FATAL,
            LogLevel::ALERT => Severity::FATAL,
            LogLevel::EMERGENCY => Severity::FATAL,
        ]));

        $psrLogger = new TelemetryLogger($logger, $converter);
        $psrLogger->debug('trace-me');

        self::assertSame(Severity::TRACE, $processor->entries()[0]->record->severity);
    }

    public function test_debug_emits_debug_severity() : void
    {
        $this->psrLogger->debug('hello');

        self::assertSame(Severity::DEBUG, $this->processor->entries()[0]->record->severity);
    }

    public function test_emergency_emits_fatal_severity() : void
    {
        $this->psrLogger->emergency('panic');

        self::assertSame(Severity::FATAL, $this->processor->entries()[0]->record->severity);
    }

    public function test_error_emits_error_severity() : void
    {
        $this->psrLogger->error('oops');

        self::assertSame(Severity::ERROR, $this->processor->entries()[0]->record->severity);
    }

    public function test_exception_in_context_routes_to_set_exception() : void
    {
        $exception = new \RuntimeException('boom');

        $this->psrLogger->error('failure', ['exception' => $exception]);

        $entry = $this->processor->entries()[0];
        self::assertSame(\RuntimeException::class, $entry->record->attributes->get('exception.type'));
        self::assertSame('boom', $entry->record->attributes->get('exception.message'));
        self::assertNotNull($entry->record->attributes->get('exception.stacktrace'));
        self::assertFalse($entry->record->attributes->has('exception'));
    }

    public function test_forwards_message_body_verbatim_when_no_placeholders() : void
    {
        $this->psrLogger->info('Hello World');

        self::assertSame('Hello World', $this->processor->entries()[0]->record->body);
    }

    public function test_info_emits_info_severity() : void
    {
        $this->psrLogger->info('hello');

        self::assertSame(Severity::INFO, $this->processor->entries()[0]->record->severity);
    }

    public function test_interpolates_placeholders_from_context() : void
    {
        $this->psrLogger->info('User {user_id} logged in', ['user_id' => 123]);

        $entry = $this->processor->entries()[0];
        self::assertSame('User 123 logged in', $entry->record->body);
        self::assertSame(123, $entry->record->attributes->get('user_id'));
    }

    #[DataProvider('level_to_severity_provider')]
    public function test_log_maps_level_string_to_severity(string $level, Severity $expected) : void
    {
        $this->psrLogger->log($level, 'msg');

        self::assertSame($expected, $this->processor->entries()[0]->record->severity);
    }

    public function test_log_throws_on_non_string_level() : void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->psrLogger->log(42, 'msg');
    }

    public function test_log_throws_on_unknown_level() : void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->psrLogger->log('verbose', 'msg');
    }

    public function test_notice_emits_info_severity() : void
    {
        $this->psrLogger->notice('note');

        self::assertSame(Severity::INFO, $this->processor->entries()[0]->record->severity);
    }

    public function test_stores_context_under_raw_keys() : void
    {
        $this->psrLogger->info('msg', ['user_id' => 1, 'role' => 'admin']);

        $entry = $this->processor->entries()[0];
        self::assertSame(1, $entry->record->attributes->get('user_id'));
        self::assertSame('admin', $entry->record->attributes->get('role'));
    }

    public function test_stringable_message_is_supported() : void
    {
        $message = new class implements \Stringable {
            public function __toString() : string
            {
                return 'rendered';
            }
        };

        $this->psrLogger->info($message);

        self::assertSame('rendered', $this->processor->entries()[0]->record->body);
    }

    public function test_warning_emits_warn_severity() : void
    {
        $this->psrLogger->warning('careful');

        self::assertSame(Severity::WARN, $this->processor->entries()[0]->record->severity);
    }
}

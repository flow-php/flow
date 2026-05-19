<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry\Tests\Integration;

use Flow\Telemetry\Logger\Severity;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use RuntimeException;

use function Flow\Bridge\Psr3\Telemetry\DSL\psr3_log_record_converter;
use function Flow\Bridge\Psr3\Telemetry\DSL\psr3_severity_mapper;
use function Flow\Bridge\Psr3\Telemetry\DSL\psr3_telemetry_logger;

final class TelemetryLoggerIntegrationTest extends TestCase
{
    public function test_basic_logging_flow_through_all_severities(): void
    {
        $context = TelemetryTestContext::create();
        $psrLogger = psr3_telemetry_logger($context->logger);

        $psrLogger->debug('debug message');
        $psrLogger->info('info message');
        $psrLogger->notice('notice message');
        $psrLogger->warning('warning message');
        $psrLogger->error('error message');
        $psrLogger->critical('critical message');
        $psrLogger->alert('alert message');
        $psrLogger->emergency('emergency message');

        $entries = $context->processor->entries();
        static::assertCount(8, $entries);

        static::assertSame(Severity::DEBUG, $entries[0]->record->severity);
        static::assertSame(Severity::INFO, $entries[1]->record->severity);
        static::assertSame(Severity::INFO, $entries[2]->record->severity);
        static::assertSame(Severity::WARN, $entries[3]->record->severity);
        static::assertSame(Severity::ERROR, $entries[4]->record->severity);
        static::assertSame(Severity::FATAL, $entries[5]->record->severity);
        static::assertSame(Severity::FATAL, $entries[6]->record->severity);
        static::assertSame(Severity::FATAL, $entries[7]->record->severity);
    }

    public function test_custom_converter_remaps_severities_through_pipeline(): void
    {
        $context = TelemetryTestContext::create();

        $converter = psr3_log_record_converter(severityMapper: psr3_severity_mapper([
            LogLevel::DEBUG => Severity::TRACE,
            LogLevel::INFO => Severity::INFO,
            LogLevel::NOTICE => Severity::WARN,
            LogLevel::WARNING => Severity::WARN,
            LogLevel::ERROR => Severity::ERROR,
            LogLevel::CRITICAL => Severity::FATAL,
            LogLevel::ALERT => Severity::FATAL,
            LogLevel::EMERGENCY => Severity::FATAL,
        ]));

        $psrLogger = psr3_telemetry_logger($context->logger, $converter);

        $psrLogger->debug('mapped to TRACE');
        $psrLogger->notice('mapped to WARN');

        $entries = $context->processor->entries();
        static::assertCount(2, $entries);
        static::assertSame(Severity::TRACE, $entries[0]->record->severity);
        static::assertSame(Severity::WARN, $entries[1]->record->severity);
    }

    public function test_exception_in_context_populates_exception_attributes(): void
    {
        $context = TelemetryTestContext::create();
        $psrLogger = psr3_telemetry_logger($context->logger);

        $psrLogger->error('Something went wrong', [
            'exception' => new RuntimeException('boom'),
            'request_id' => 'req-1',
        ]);

        $entry = $context->processor->entries()[0];
        static::assertSame(RuntimeException::class, $entry->record->attributes->get('exception.type'));
        static::assertSame('boom', $entry->record->attributes->get('exception.message'));
        static::assertNotNull($entry->record->attributes->get('exception.stacktrace'));
        static::assertSame('req-1', $entry->record->attributes->get('request_id'));
        static::assertFalse($entry->record->attributes->has('exception'));
    }

    public function test_implements_psr3_logger_interface(): void
    {
        $context = TelemetryTestContext::create();

        static::assertInstanceOf(LoggerInterface::class, psr3_telemetry_logger($context->logger));
    }

    public function test_message_interpolation_and_attribute_storage_combined(): void
    {
        $context = TelemetryTestContext::create();
        $psrLogger = psr3_telemetry_logger($context->logger);

        $psrLogger->info('User {user_id} performed {action} from {ip}', [
            'user_id' => 42,
            'action' => 'login',
            'ip' => '192.168.1.1',
        ]);

        $entry = $context->processor->entries()[0];
        static::assertSame('User 42 performed login from 192.168.1.1', $entry->record->body);
        static::assertSame(42, $entry->record->attributes->get('user_id'));
        static::assertSame('login', $entry->record->attributes->get('action'));
        static::assertSame('192.168.1.1', $entry->record->attributes->get('ip'));
    }
}

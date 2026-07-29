<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Processor;

use DateTimeImmutable;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogRecord;
use Flow\Telemetry\Logger\Processor\BatchingLogProcessor;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\ExporterSpy;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class BatchingLogProcessorTest extends TestCase
{
    private Resource $resource;

    protected function setUp(): void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_exports_on_batch_size_reached(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::once())
            ->method('export')
            ->with(static::callback(
                static fn(mixed $signal) => (
                    $signal instanceof Signals
                    && $signal->type === SignalType::LOGS
                    && $signal->count() === 2
                ),
            ))
            ->willReturn(true);

        $processor = new BatchingLogProcessor($exporter, 2);

        $processor->process($this->createEntry(Severity::INFO, 'Log message 1'));
        $processor->process($this->createEntry(Severity::INFO, 'Log message 2'));
    }

    public function test_exports_remaining_on_flush(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::once())
            ->method('export')
            ->with(static::callback(
                static fn(mixed $signal) => (
                    $signal instanceof Signals
                    && $signal->type === SignalType::LOGS
                    && $signal->count() === 1
                ),
            ))
            ->willReturn(true);

        $processor = new BatchingLogProcessor($exporter, 10);
        $processor->process($this->createEntry(Severity::INFO, 'Log message'));

        $result = $processor->flush();

        static::assertTrue($result);
    }

    public function test_flush_clears_buffer_after_exporter_throws(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::once())
            ->method('export')
            ->willThrowException(new RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new BatchingLogProcessor($exporter, 10, $spy);
        $processor->process(LogEntryMother::create('msg', Severity::INFO));
        $processor->flush();
        $processor->flush();

        static::assertSame(1, $spy->count());
    }

    public function test_flush_returns_true_when_buffer_empty(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->expects(self::never())->method('export');

        $processor = new BatchingLogProcessor($exporter, 10);

        $result = $processor->flush();

        static::assertTrue($result);
    }

    public function test_flush_routes_exporter_throwable_to_error_handler(): void
    {
        $exporter = $this->createStub(Exporter::class);
        $exporter->method('export')->willThrowException(new RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new BatchingLogProcessor($exporter, 10, $spy);
        $processor->process(LogEntryMother::create('msg', Severity::INFO));

        $result = $processor->flush();

        static::assertFalse($result);
        static::assertSame(1, $spy->count());
        $last = $spy->last();
        static::assertInstanceOf(RuntimeException::class, $last);
        static::assertSame('exporter exploded', $last->getMessage());
    }

    public function test_process_stores_all_log_record_fields(): void
    {
        $capturedSignal = null;
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::once())
            ->method('export')
            ->with(static::callback(static function (mixed $signal) use (&$capturedSignal): bool {
                if ($signal instanceof Signals) {
                    $capturedSignal = $signal;
                }

                return $signal instanceof Signals && $signal->type === SignalType::LOGS;
            }))
            ->willReturn(true);

        $scope = new InstrumentationScope('test-scope', '1.0.0');
        $timestamp = new DateTimeImmutable('2024-01-01 12:00:00');
        $attributes = ['user.id' => 'test-user', 'request.id' => '12345'];

        $processor = new BatchingLogProcessor($exporter, 10);
        $processor->process(
            new LogEntry(
                (new LogRecord())
                    ->setSeverity(Severity::ERROR)
                    ->setBody('Error message')
                    ->setAttributes($attributes),
                $this->resource,
                $scope,
                $timestamp,
            ),
        );
        $processor->flush();

        static::assertInstanceOf(Signals::class, $capturedSignal);
        static::assertSame(SignalType::LOGS, $capturedSignal->type);
        static::assertCount(1, $capturedSignal->allLogs());
        $entry = $capturedSignal->allLogs()[0];
        static::assertSame($scope, $entry->scope);
        static::assertSame(Severity::ERROR, $entry->record->severity);
        static::assertSame('Error message', $entry->record->body);
        static::assertSame($attributes, $entry->record->attributes->normalize());
        static::assertSame($timestamp, $entry->timestamp);
    }

    public function test_age_disabled_by_default_does_not_export_until_flush(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingLogProcessor($exporter, 512, new ErrorHandlerSpy(), null);

        $processor->process(LogEntryMother::create('msg', Severity::INFO));
        $processor->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame(0, $exporter->exportedCount());

        static::assertTrue($processor->flush());
        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_age_set_but_not_due_does_not_export_until_flush(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingLogProcessor($exporter, 512, new ErrorHandlerSpy(), 3600.0);

        $processor->process(LogEntryMother::create('msg', Severity::INFO));
        $processor->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame(0, $exporter->exportedCount());

        static::assertTrue($processor->flush());
        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_age_flush_when_deadline_exceeded_on_process(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingLogProcessor($exporter, 512, new ErrorHandlerSpy(), 0.01);

        $processor->process(LogEntryMother::create('msg', Severity::INFO));
        usleep(20_000);
        $processor->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_size_trigger_still_flushes_when_age_set_but_not_due(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingLogProcessor($exporter, 2, new ErrorHandlerSpy(), 3600.0);

        $processor->process(LogEntryMother::create('msg', Severity::INFO));
        $processor->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_age_clock_resets_after_flush(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingLogProcessor($exporter, 512, new ErrorHandlerSpy(), 0.01);

        $processor->process(LogEntryMother::create('msg', Severity::INFO));
        usleep(20_000);
        static::assertTrue($processor->flush());
        static::assertSame(1, $exporter->exportedCount());

        $processor->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame(1, $exporter->exportedCount());
    }

    private function createEntry(Severity $severity, string $body): LogEntry
    {
        return new LogEntry(
            (new LogRecord())
                ->setSeverity($severity)
                ->setBody($body),
            $this->resource,
            new InstrumentationScope('test'),
            new DateTimeImmutable(),
        );
    }
}

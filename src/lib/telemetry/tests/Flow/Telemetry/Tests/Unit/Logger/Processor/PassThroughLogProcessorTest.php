<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Processor;

use DateTimeImmutable;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogRecord;
use Flow\Telemetry\Logger\Processor\PassThroughLogProcessor;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class PassThroughLogProcessorTest extends TestCase
{
    private Resource $resource;

    protected function setUp(): void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_exports_each_log_individually(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::exactly(3))
            ->method('export')
            ->with(static::callback(
                static fn(mixed $signal) => (
                    $signal instanceof Signals
                    && $signal->type === SignalType::LOGS
                    && $signal->count() === 1
                ),
            ))
            ->willReturn(true);

        $processor = new PassThroughLogProcessor($exporter);

        for ($i = 0; $i < 3; $i++) {
            $processor->process($this->createEntry(Severity::INFO, 'test message ' . $i));
        }
    }

    public function test_exports_log_immediately_on_process(): void
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

        $processor = new PassThroughLogProcessor($exporter);
        $processor->process($this->createEntry(Severity::INFO, 'test message', ['key' => 'value']));
    }

    public function test_flush_returns_true(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $processor = new PassThroughLogProcessor($exporter);

        $result = $processor->flush();

        static::assertTrue($result);
    }

    public function test_process_routes_exporter_throwable_to_error_handler(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->method('export')->willThrowException(new RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new PassThroughLogProcessor($exporter, $spy);
        $processor->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame(1, $spy->count());
        static::assertSame('exporter exploded', $spy->last()?->getMessage());
    }

    /**
     * @param array<string, array<bool|\DateTimeImmutable|float|int|string>|bool|\DateTimeImmutable|float|int|string> $attributes
     */
    private function createEntry(Severity $severity, string $body, array $attributes = []): LogEntry
    {
        return new LogEntry(
            (new LogRecord())
                ->setSeverity($severity)
                ->setBody($body)
                ->setAttributes($attributes),
            $this->resource,
            new InstrumentationScope('test', '1.0.0'),
            new DateTimeImmutable(),
        );
    }
}

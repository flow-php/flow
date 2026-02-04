<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Processor;

use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogProcessor, LogRecord, Severity};
use Flow\Telemetry\Logger\Processor\SeverityFilteringLogProcessor;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class SeverityFilteringLogProcessorTest extends TestCase
{
    private Resource $resource;

    protected function setUp() : void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_exporter_returns_wrapped_processor_exporter() : void
    {
        $exporter = $this->createMock(LogExporter::class);
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::once())
            ->method('exporter')
            ->willReturn($exporter);

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::INFO);

        self::assertSame($exporter, $processor->exporter());
    }

    public function test_filters_multiple_entries_correctly() : void
    {
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::exactly(3))
            ->method('process');

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::WARN);

        $processor->process($this->createEntry(Severity::TRACE, 'trace message'));
        $processor->process($this->createEntry(Severity::DEBUG, 'debug message'));
        $processor->process($this->createEntry(Severity::INFO, 'info message'));
        $processor->process($this->createEntry(Severity::WARN, 'warn message'));
        $processor->process($this->createEntry(Severity::ERROR, 'error message'));
        $processor->process($this->createEntry(Severity::FATAL, 'fatal message'));
    }

    public function test_filters_out_log_below_minimum_severity_level() : void
    {
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::never())
            ->method('process');

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::WARN);

        $processor->process($this->createEntry(Severity::INFO, 'info message'));
    }

    public function test_filters_trace_when_minimum_is_debug() : void
    {
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::never())
            ->method('process');

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::DEBUG);

        $processor->process($this->createEntry(Severity::TRACE, 'trace message'));
    }

    public function test_flush_delegates_to_wrapped_processor() : void
    {
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::once())
            ->method('flush')
            ->willReturn(true);

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::INFO);

        self::assertTrue($processor->flush());
    }

    public function test_flush_returns_false_when_wrapped_processor_fails() : void
    {
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::once())
            ->method('flush')
            ->willReturn(false);

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::INFO);

        self::assertFalse($processor->flush());
    }

    public function test_only_fatal_when_minimum_is_fatal() : void
    {
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::once())
            ->method('process');

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::FATAL);

        $processor->process($this->createEntry(Severity::TRACE, 'trace'));
        $processor->process($this->createEntry(Severity::DEBUG, 'debug'));
        $processor->process($this->createEntry(Severity::INFO, 'info'));
        $processor->process($this->createEntry(Severity::WARN, 'warn'));
        $processor->process($this->createEntry(Severity::ERROR, 'error'));
        $processor->process($this->createEntry(Severity::FATAL, 'fatal'));
    }

    public function test_passes_all_levels_when_minimum_is_trace() : void
    {
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::exactly(6))
            ->method('process');

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::TRACE);

        $processor->process($this->createEntry(Severity::TRACE, 'trace'));
        $processor->process($this->createEntry(Severity::DEBUG, 'debug'));
        $processor->process($this->createEntry(Severity::INFO, 'info'));
        $processor->process($this->createEntry(Severity::WARN, 'warn'));
        $processor->process($this->createEntry(Severity::ERROR, 'error'));
        $processor->process($this->createEntry(Severity::FATAL, 'fatal'));
    }

    public function test_passes_through_log_above_minimum_severity_level() : void
    {
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::once())
            ->method('process');

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::WARN);

        $processor->process($this->createEntry(Severity::ERROR, 'error message'));
    }

    public function test_passes_through_log_at_minimum_severity_level() : void
    {
        $wrappedProcessor = $this->createMock(LogProcessor::class);
        $wrappedProcessor->expects(self::once())
            ->method('process');

        $processor = new SeverityFilteringLogProcessor($wrappedProcessor, Severity::WARN);

        $processor->process($this->createEntry(Severity::WARN, 'warning message'));
    }

    private function createEntry(Severity $severity, string $body) : LogEntry
    {
        return new LogEntry(
            (new LogRecord())->setSeverity($severity)->setBody($body),
            $this->resource,
            new InstrumentationScope('test', '1.0.0'),
            new \DateTimeImmutable(),
        );
    }
}

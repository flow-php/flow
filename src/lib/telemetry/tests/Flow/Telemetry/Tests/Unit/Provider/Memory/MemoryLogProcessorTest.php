<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Memory;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogProcessor;
use Flow\Telemetry\Logger\LogRecord;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class MemoryLogProcessorTest extends TestCase
{
    public function test_count_logs_returns_correct_count(): void
    {
        $processor = new MemoryLogProcessor(new MemoryExporter());

        static::assertSame(0, $processor->countLogs());

        $processor->process($this->createLogEntry('Log 1', Severity::INFO));
        static::assertSame(1, $processor->countLogs());

        $processor->process($this->createLogEntry('Log 2', Severity::WARN));
        static::assertSame(2, $processor->countLogs());
    }

    public function test_entries_containing_filters_correctly(): void
    {
        $processor = new MemoryLogProcessor(new MemoryExporter());
        $entry1 = $this->createLogEntry('User logged in successfully', Severity::INFO);
        $entry2 = $this->createLogEntry('User logged out', Severity::INFO);
        $entry3 = $this->createLogEntry('Database connection failed', Severity::ERROR);

        $processor->process($entry1);
        $processor->process($entry2);
        $processor->process($entry3);

        $userLogs = $processor->entriesContaining('User');
        static::assertCount(2, $userLogs);
        static::assertSame($entry1, $userLogs[0]);
        static::assertSame($entry2, $userLogs[1]);

        $databaseLogs = $processor->entriesContaining('Database');
        static::assertCount(1, $databaseLogs);
        static::assertSame($entry3, $databaseLogs[0]);
    }

    public function test_entries_returns_all_processed_entries(): void
    {
        $processor = new MemoryLogProcessor(new MemoryExporter());
        $entry1 = $this->createLogEntry('Log 1', Severity::INFO);
        $entry2 = $this->createLogEntry('Log 2', Severity::WARN);

        $processor->process($entry1);
        $processor->process($entry2);

        static::assertCount(2, $processor->entries());
        static::assertSame($entry1, $processor->entries()[0]);
        static::assertSame($entry2, $processor->entries()[1]);
    }

    public function test_entries_with_severity_filters_correctly(): void
    {
        $processor = new MemoryLogProcessor(new MemoryExporter());
        $infoEntry = $this->createLogEntry('Info message', Severity::INFO);
        $warnEntry = $this->createLogEntry('Warning message', Severity::WARN);
        $errorEntry = $this->createLogEntry('Error message', Severity::ERROR);

        $processor->process($infoEntry);
        $processor->process($warnEntry);
        $processor->process($errorEntry);

        $infoLogs = $processor->entriesWithSeverity(Severity::INFO);
        static::assertCount(1, $infoLogs);
        static::assertSame($infoEntry, $infoLogs[0]);

        $warnLogs = $processor->entriesWithSeverity(Severity::WARN);
        static::assertCount(1, $warnLogs);
        static::assertSame($warnEntry, $warnLogs[0]);

        $errorLogs = $processor->entriesWithSeverity(Severity::ERROR);
        static::assertCount(1, $errorLogs);
        static::assertSame($errorEntry, $errorLogs[0]);
    }

    public function test_flush_exports_entries(): void
    {
        $exporter = new MemoryExporter();
        $processor = new MemoryLogProcessor($exporter);
        $entry = $this->createLogEntry('Test message', Severity::INFO);

        $processor->process($entry);
        $result = $processor->flush();

        static::assertTrue($result);
        static::assertCount(1, $exporter->logs());
        static::assertSame($entry, $exporter->logs()[0]);
    }

    public function test_flush_returns_true_when_no_entries(): void
    {
        $processor = new MemoryLogProcessor(new MemoryExporter());

        static::assertTrue($processor->flush());
    }

    public function test_flush_routes_exporter_throwable_to_error_handler(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->method('export')->willThrowException(new \RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new MemoryLogProcessor($exporter, $spy);
        $processor->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertFalse($processor->flush());
        static::assertSame(1, $spy->count());
        static::assertSame('exporter exploded', $spy->last()?->getMessage());
    }

    public function test_implements_log_processor(): void
    {
        static::assertInstanceOf(LogProcessor::class, new MemoryLogProcessor(new MemoryExporter()));
    }

    public function test_process_stores_entry(): void
    {
        $processor = new MemoryLogProcessor(new MemoryExporter());
        $entry = $this->createLogEntry('Test message', Severity::INFO);

        $processor->process($entry);

        static::assertCount(1, $processor->entries());
        static::assertSame($entry, $processor->entries()[0]);
    }

    public function test_reset_clears_all_entries(): void
    {
        $processor = new MemoryLogProcessor(new MemoryExporter());

        $processor->process($this->createLogEntry('Log 1', Severity::INFO));
        $processor->process($this->createLogEntry('Log 2', Severity::WARN));

        static::assertSame(2, $processor->countLogs());

        $processor->reset();

        static::assertSame(0, $processor->countLogs());
        static::assertSame([], $processor->entries());
    }

    private function createLogEntry(string $body, Severity $severity): LogEntry
    {
        return new LogEntry(
            (new LogRecord())
                ->setSeverity($severity)
                ->setBody($body),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            new \DateTimeImmutable(),
        );
    }
}

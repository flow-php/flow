<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Memory;

use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogRecord, Severity};
use Flow\Telemetry\Provider\Memory\MemoryLogExporter;
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, ResourceMother};
use Flow\Telemetry\Transport\VoidTransport;
use PHPUnit\Framework\TestCase;

final class MemoryLogExporterTest extends TestCase
{
    public function test_export_empty_entries_returns_true() : void
    {
        $exporter = new MemoryLogExporter();

        self::assertTrue($exporter->export([]));
        self::assertSame([], $exporter->entries());
    }

    public function test_export_multiple_entries() : void
    {
        $exporter = new MemoryLogExporter();
        $entry1 = $this->createLogEntry('Log 1', Severity::INFO);
        $entry2 = $this->createLogEntry('Log 2', Severity::WARN);
        $entry3 = $this->createLogEntry('Log 3', Severity::ERROR);

        $exporter->export([$entry1, $entry2]);
        $exporter->export([$entry3]);

        self::assertCount(3, $exporter->entries());
        self::assertSame($entry1, $exporter->entries()[0]);
        self::assertSame($entry2, $exporter->entries()[1]);
        self::assertSame($entry3, $exporter->entries()[2]);
    }

    public function test_export_single_entry() : void
    {
        $exporter = new MemoryLogExporter();
        $entry = $this->createLogEntry('Test message', Severity::INFO);

        $result = $exporter->export([$entry]);

        self::assertTrue($result);
        self::assertCount(1, $exporter->entries());
        self::assertSame($entry, $exporter->entries()[0]);
    }

    public function test_implements_log_exporter() : void
    {
        self::assertInstanceOf(LogExporter::class, new MemoryLogExporter());
    }

    public function test_reset_clears_all_entries() : void
    {
        $exporter = new MemoryLogExporter();
        $exporter->export([
            $this->createLogEntry('Log 1', Severity::INFO),
            $this->createLogEntry('Log 2', Severity::WARN),
        ]);

        self::assertCount(2, $exporter->entries());

        $exporter->reset();

        self::assertSame([], $exporter->entries());
    }

    public function test_transports_returns_void_transport() : void
    {
        $exporter = new MemoryLogExporter();
        $transports = $exporter->transports();

        self::assertCount(1, $transports);
        self::assertInstanceOf(VoidTransport::class, $transports[0]);
    }

    private function createLogEntry(string $body, Severity $severity) : LogEntry
    {
        return new LogEntry(
            (new LogRecord())->setSeverity($severity)->setBody($body),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            new \DateTimeImmutable(),
        );
    }
}

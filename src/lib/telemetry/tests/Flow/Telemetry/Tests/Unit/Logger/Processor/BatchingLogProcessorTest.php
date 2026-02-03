<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Processor;

use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogRecord, Severity};
use Flow\Telemetry\Logger\Processor\BatchingLogProcessor;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class BatchingLogProcessorTest extends TestCase
{
    private Resource $resource;

    protected function setUp() : void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_exports_on_batch_size_reached() : void
    {
        $exporter = $this->createMock(LogExporter::class);
        $exporter->expects(self::once())
            ->method('export')
            ->with(self::callback(static fn (array $entries) => \count($entries) === 2))
            ->willReturn(true);

        $processor = new BatchingLogProcessor($exporter, 2);

        $processor->process($this->createEntry(Severity::INFO, 'Log message 1'));
        $processor->process($this->createEntry(Severity::INFO, 'Log message 2'));
    }

    public function test_exports_remaining_on_flush() : void
    {
        $exporter = $this->createMock(LogExporter::class);
        $exporter->expects(self::once())
            ->method('export')
            ->with(self::callback(static fn (array $entries) => \count($entries) === 1))
            ->willReturn(true);

        $processor = new BatchingLogProcessor($exporter, 10);
        $processor->process($this->createEntry(Severity::INFO, 'Log message'));

        $result = $processor->flush();

        self::assertTrue($result);
    }

    public function test_flush_returns_true_when_buffer_empty() : void
    {
        $exporter = $this->createMock(LogExporter::class);
        $exporter->expects(self::never())
            ->method('export');

        $processor = new BatchingLogProcessor($exporter, 10);

        $result = $processor->flush();

        self::assertTrue($result);
    }

    public function test_process_stores_all_log_record_fields() : void
    {
        $exportedEntries = null;
        $exporter = $this->createMock(LogExporter::class);
        $exporter->expects(self::once())
            ->method('export')
            ->with(self::callback(static function (array $entries) use (&$exportedEntries) {
                $exportedEntries = $entries;

                return true;
            }))
            ->willReturn(true);

        $scope = new InstrumentationScope('test-scope', '1.0.0');
        $timestamp = new \DateTimeImmutable('2024-01-01 12:00:00');
        $attributes = ['user.id' => 'test-user', 'request.id' => '12345'];

        $processor = new BatchingLogProcessor($exporter, 10);
        $processor->process(new LogEntry(
            (new LogRecord())
                ->setSeverity(Severity::ERROR)
                ->setBody('Error message')
                ->setAttributes($attributes),
            $this->resource,
            $scope,
            $timestamp,
        ));
        $processor->flush();

        self::assertNotNull($exportedEntries);
        self::assertCount(1, $exportedEntries);
        $entry = $exportedEntries[0];
        self::assertSame($scope, $entry->scope);
        self::assertSame(Severity::ERROR, $entry->record->severity);
        self::assertSame('Error message', $entry->record->body);
        self::assertSame($attributes, $entry->record->attributes->normalize());
        self::assertSame($timestamp, $entry->timestamp);
    }

    private function createEntry(Severity $severity, string $body) : LogEntry
    {
        return new LogEntry(
            (new LogRecord())->setSeverity($severity)->setBody($body),
            $this->resource,
            new InstrumentationScope('test'),
            new \DateTimeImmutable(),
        );
    }
}

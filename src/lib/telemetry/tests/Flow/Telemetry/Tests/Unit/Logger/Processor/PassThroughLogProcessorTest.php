<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Processor;

use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogRecord, Severity};
use Flow\Telemetry\Logger\Processor\PassThroughLogProcessor;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class PassThroughLogProcessorTest extends TestCase
{
    private Resource $resource;

    protected function setUp() : void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_exports_each_log_individually() : void
    {
        $exporter = $this->createMock(LogExporter::class);
        $exporter->expects(self::exactly(3))
            ->method('export')
            ->with(self::callback(static fn (array $entries) => \count($entries) === 1))
            ->willReturn(true);

        $processor = new PassThroughLogProcessor($exporter);

        for ($i = 0; $i < 3; $i++) {
            $processor->process($this->createEntry(Severity::INFO, 'test message ' . $i));
        }
    }

    public function test_exports_log_immediately_on_process() : void
    {
        $exporter = $this->createMock(LogExporter::class);
        $exporter->expects(self::once())
            ->method('export')
            ->with(self::callback(static fn (array $entries) => \count($entries) === 1))
            ->willReturn(true);

        $processor = new PassThroughLogProcessor($exporter);
        $processor->process($this->createEntry(Severity::INFO, 'test message', ['key' => 'value']));
    }

    public function test_flush_returns_true() : void
    {
        $exporter = $this->createMock(LogExporter::class);
        $processor = new PassThroughLogProcessor($exporter);

        $result = $processor->flush();

        self::assertTrue($result);
    }

    /**
     * @param array<string, array<bool|\DateTimeImmutable|float|int|string>|bool|\DateTimeImmutable|float|int|string> $attributes
     */
    private function createEntry(Severity $severity, string $body, array $attributes = []) : LogEntry
    {
        return new LogEntry(
            (new LogRecord())->setSeverity($severity)->setBody($body)->setAttributes($attributes),
            $this->resource,
            new InstrumentationScope('test', '1.0.0'),
            new \DateTimeImmutable(),
        );
    }
}

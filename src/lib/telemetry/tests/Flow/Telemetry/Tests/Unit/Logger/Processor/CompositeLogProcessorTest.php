<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Processor;

use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\{LogEntry, LogProcessor, LogRecord, Severity};
use Flow\Telemetry\Logger\Processor\CompositeLogProcessor;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class CompositeLogProcessorTest extends TestCase
{
    public function test_flush_returns_false_when_any_fails() : void
    {
        $processor1 = $this->createMock(LogProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(LogProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(false);

        $composite = new CompositeLogProcessor([$processor1, $processor2]);

        self::assertFalse($composite->flush());
    }

    public function test_flush_returns_true_when_all_succeed() : void
    {
        $processor1 = $this->createMock(LogProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(LogProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(true);

        $composite = new CompositeLogProcessor([$processor1, $processor2]);

        self::assertTrue($composite->flush());
    }

    public function test_forwards_process_to_all_processors() : void
    {
        $processor1 = $this->createMock(LogProcessor::class);
        $processor1->expects(self::once())->method('process');

        $processor2 = $this->createMock(LogProcessor::class);
        $processor2->expects(self::once())->method('process');

        $composite = new CompositeLogProcessor([$processor1, $processor2]);
        $composite->process($this->createEntry());
    }

    public function test_works_with_empty_processors_array() : void
    {
        $composite = new CompositeLogProcessor([]);

        $composite->process($this->createEntry());

        self::assertTrue($composite->flush());
    }

    private function createEntry() : LogEntry
    {
        return new LogEntry(
            (new LogRecord())->setSeverity(Severity::INFO)->setBody('test message'),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            new \DateTimeImmutable(),
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger\Processor;

use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogProcessor;
use Flow\Telemetry\Logger\LogRecord;
use Flow\Telemetry\Logger\Processor\CompositeLogProcessor;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\LogEntryMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class CompositeLogProcessorTest extends TestCase
{
    public function test_flush_continues_after_child_throws_and_routes_to_error_handler(): void
    {
        $throwing = $this->createMock(LogProcessor::class);
        $throwing->method('flush')->willThrowException(new \RuntimeException('flush blew up'));

        $sibling = $this->createMock(LogProcessor::class);
        $sibling->expects(self::once())->method('flush')->willReturn(true);

        $spy = new ErrorHandlerSpy();
        $composite = new CompositeLogProcessor([$throwing, $sibling], $spy);

        static::assertFalse($composite->flush());
        static::assertSame(1, $spy->count());
    }

    public function test_flush_returns_false_when_any_fails(): void
    {
        $processor1 = $this->createMock(LogProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(LogProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(false);

        $composite = new CompositeLogProcessor([$processor1, $processor2]);

        static::assertFalse($composite->flush());
    }

    public function test_flush_returns_true_when_all_succeed(): void
    {
        $processor1 = $this->createMock(LogProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(LogProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(true);

        $composite = new CompositeLogProcessor([$processor1, $processor2]);

        static::assertTrue($composite->flush());
    }

    public function test_forwards_process_to_all_processors(): void
    {
        $processor1 = $this->createMock(LogProcessor::class);
        $processor1->expects(self::once())->method('process');

        $processor2 = $this->createMock(LogProcessor::class);
        $processor2->expects(self::once())->method('process');

        $composite = new CompositeLogProcessor([$processor1, $processor2]);
        $composite->process($this->createEntry());
    }

    public function test_process_continues_after_child_throws_and_routes_to_error_handler(): void
    {
        $throwing = $this->createMock(LogProcessor::class);
        $throwing->method('process')->willThrowException(new \RuntimeException('child blew up'));

        $sibling = $this->createMock(LogProcessor::class);
        $sibling->expects(self::once())->method('process');

        $spy = new ErrorHandlerSpy();
        $composite = new CompositeLogProcessor([$throwing, $sibling], $spy);
        $composite->process(LogEntryMother::create('msg', Severity::INFO));

        static::assertSame(1, $spy->count());
        static::assertSame('child blew up', $spy->last()?->getMessage());
    }

    public function test_works_with_empty_processors_array(): void
    {
        $composite = new CompositeLogProcessor([]);

        $composite->process($this->createEntry());

        static::assertTrue($composite->flush());
    }

    private function createEntry(): LogEntry
    {
        return new LogEntry(
            (new LogRecord())
                ->setSeverity(Severity::INFO)
                ->setBody('test message'),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            new \DateTimeImmutable(),
        );
    }
}

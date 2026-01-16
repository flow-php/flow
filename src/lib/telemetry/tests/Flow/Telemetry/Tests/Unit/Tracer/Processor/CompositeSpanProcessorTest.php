<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Processor;

use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Processor\CompositeSpanProcessor;
use Flow\Telemetry\Tracer\{Span, SpanContext, SpanKind, SpanProcessor};
use PHPUnit\Framework\TestCase;

final class CompositeSpanProcessorTest extends TestCase
{
    public function test_flush_returns_false_when_any_fails() : void
    {
        $processor1 = $this->createMock(SpanProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(SpanProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(false);

        $composite = new CompositeSpanProcessor([$processor1, $processor2]);

        self::assertFalse($composite->flush());
    }

    public function test_flush_returns_true_when_all_succeed() : void
    {
        $processor1 = $this->createMock(SpanProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(SpanProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(true);

        $composite = new CompositeSpanProcessor([$processor1, $processor2]);

        self::assertTrue($composite->flush());
    }

    public function test_forwards_on_end_to_all_processors() : void
    {
        $processor1 = $this->createMock(SpanProcessor::class);
        $processor1->expects(self::once())->method('onEnd');

        $processor2 = $this->createMock(SpanProcessor::class);
        $processor2->expects(self::once())->method('onEnd');

        $composite = new CompositeSpanProcessor([$processor1, $processor2]);
        $composite->onEnd($this->createSpan());
    }

    public function test_forwards_on_start_to_all_processors() : void
    {
        $processor1 = $this->createMock(SpanProcessor::class);
        $processor1->expects(self::once())->method('onStart');

        $processor2 = $this->createMock(SpanProcessor::class);
        $processor2->expects(self::once())->method('onStart');

        $composite = new CompositeSpanProcessor([$processor1, $processor2]);
        $composite->onStart($this->createSpan());
    }

    public function test_works_with_empty_processors_array() : void
    {
        $composite = new CompositeSpanProcessor([]);

        $composite->onStart($this->createSpan());
        $composite->onEnd($this->createSpan());

        self::assertTrue($composite->flush());
    }

    private function createSpan() : Span
    {
        return new Span(
            'test-span',
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0')
        );
    }
}

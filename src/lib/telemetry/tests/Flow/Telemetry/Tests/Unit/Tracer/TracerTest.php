<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tests\Mother\TracerMother;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanProcessor;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function Flow\Telemetry\DSL\always_off_sampler;
use function Flow\Telemetry\DSL\always_on_sampler;
use function Flow\Telemetry\DSL\parent_based_sampler;

final class TracerTest extends TestCase
{
    public function test_active_span_returns_current_span_context(): void
    {
        $tracer = TracerMother::create();
        $span = $tracer->span('test-span');
        $tracer->activate($span);

        $active = $tracer->activeSpan();
        static::assertNotNull($active);
        static::assertTrue($active->spanId->equals($span->context()->spanId));
    }

    public function test_active_span_is_unchanged_by_an_unactivated_span(): void
    {
        $tracer = TracerMother::create();
        $tracer->span('test-span');

        static::assertNull($tracer->activeSpan());
    }

    public function test_active_span_returns_null_when_no_active_span(): void
    {
        static::assertNull(TracerMother::create()->activeSpan());
    }

    public function test_attached_context_active_span_wins_as_parent(): void
    {
        $storage = new MemoryContextStorage();
        $tracer = TracerMother::withContextStorage($storage);

        $attached = SpanContext::create(TraceId::generate(), SpanId::generate());
        $storage->attach(Context::root()->withActiveSpan($attached));

        $span = $tracer->span('child');

        $parentSpanId = $span->context()->parentSpanId;
        static::assertNotNull($parentSpanId);
        static::assertTrue($parentSpanId->equals($attached->spanId));
        static::assertTrue($span->context()->traceId->equals($attached->traceId));
    }

    public function test_interleaved_cross_tracer_spans_parent_to_innermost_current_span(): void
    {
        $storage = new MemoryContextStorage();
        $provider = new TracerProvider(new VoidSpanProcessor(), ClockMother::frozen(), $storage);
        $resource = ResourceMother::default();

        $tracerA = $provider->tracer($resource, 'tracerA');
        $tracerB = $provider->tracer($resource, 'tracerB');

        $a1 = $tracerA->span('A1');
        $tracerA->activate($a1);
        $b1 = $tracerB->span('B1');
        $tracerB->activate($b1);
        $a2 = $tracerA->span('A2');

        $parentSpanId = $a2->context()->parentSpanId;
        static::assertNotNull($parentSpanId);
        static::assertTrue($parentSpanId->equals($b1->context()->spanId));
        static::assertFalse($parentSpanId->equals($a1->context()->spanId));
        static::assertTrue($a2->context()->traceId->equals($a1->context()->traceId));
    }

    public function test_complete_calls_processor_on_end(): void
    {
        $processor = $this->createMock(SpanProcessor::class);
        $processor->expects(self::once())->method('onEnd');

        $tracer = TracerMother::withProcessor($processor);
        $tracer->complete($tracer->span('test-span'));
    }

    public function test_complete_is_idempotent_and_exports_once(): void
    {
        $processor = $this->createMock(SpanProcessor::class);
        $processor->expects(self::once())->method('onEnd');

        $tracer = TracerMother::withProcessor($processor);
        $span = $tracer->span('test-span');

        $tracer->complete($span);
        $tracer->complete($span);
        $tracer->complete($span);
    }

    public function test_complete_exports_a_span_that_was_ended_by_the_caller(): void
    {
        $processor = $this->createMock(SpanProcessor::class);
        $processor->expects(self::once())->method('onEnd');

        $tracer = TracerMother::withProcessor($processor);
        $span = $tracer->span('test-span');
        // callers end a span early to read its duration, then complete it
        $span->end();

        $tracer->complete($span);
    }

    public function test_complete_ends_span(): void
    {
        $tracer = TracerMother::create();
        $span = $tracer->span('test-span');

        static::assertFalse($span->isEnded());

        $tracer->complete($span);

        static::assertTrue($span->isEnded());
    }

    public function test_detaching_a_child_scope_restores_the_parent_context(): void
    {
        $tracer = TracerMother::create();
        $parent = $tracer->span('parent');
        $tracer->activate($parent);

        static::assertNotNull($tracer->activeSpan());

        foreach (['first-child', 'second-child'] as $name) {
            $child = $tracer->span($name);
            $scope = $tracer->activate($child);
            $scope->detach();
            $tracer->complete($child);
        }

        $active = $tracer->activeSpan();
        static::assertNotNull($active);
        static::assertTrue($active->spanId->equals($parent->context()->spanId));
    }

    public function test_complete_does_not_detach_the_scope(): void
    {
        $tracer = TracerMother::create();
        $span = $tracer->span('test-span');
        $tracer->activate($span);

        $tracer->complete($span);

        $active = $tracer->activeSpan();
        static::assertNotNull($active);
        static::assertTrue($active->spanId->equals($span->context()->spanId));
    }

    public function test_explicit_parent_context_is_passed_to_the_sampler(): void
    {
        $storage = new MemoryContextStorage();
        $tracer = TracerMother::withContextStorage($storage);

        // ambient is suppressed, the explicitly declared parent is not
        $storage->attach(Context::root()->withSuppressedTracing());

        static::assertTrue($tracer->span('child', parent: Context::root())->isRecording());
    }

    public function test_ambient_context_is_passed_to_the_sampler_when_no_parent_is_given(): void
    {
        $storage = new MemoryContextStorage();
        $tracer = TracerMother::withContextStorage($storage);

        $storage->attach(Context::root()->withSuppressedTracing());

        static::assertFalse($tracer->span('child')->isRecording());
    }

    public function test_root_span_keeps_suppression_from_the_ambient_context(): void
    {
        $storage = new MemoryContextStorage();
        $tracer = TracerMother::withContextStorage($storage);

        $storage->attach(Context::root()->withSuppressedTracing());

        static::assertFalse($tracer->span('root', parent: false)->isRecording());
    }

    public function test_context_returns_tracer_context(): void
    {
        $span = SpanContext::create(TraceId::generate(), SpanId::generate());
        $context = Context::root()->withActiveSpan($span);
        $tracer = TracerMother::withContext($context);

        static::assertSame($span, $tracer->context()->activeSpan());
    }

    public function test_name_returns_tracer_name(): void
    {
        static::assertSame('test-tracer', TracerMother::create()->name());
    }

    public function test_nested_spans_have_parent_child_relationship(): void
    {
        $tracer = TracerMother::create();
        $parent = $tracer->span('parent');
        $tracer->activate($parent);
        $child = $tracer->span('child');

        $parentSpanId = $child->context()->parentSpanId;
        static::assertNotNull($parentSpanId);
        static::assertTrue($parentSpanId->equals($parent->context()->spanId));
    }

    public function test_unactivated_spans_are_siblings_not_parent_and_child(): void
    {
        $tracer = TracerMother::create();
        $first = $tracer->span('first');
        $second = $tracer->span('second');

        static::assertNull($second->context()->parentSpanId);
        static::assertNull($first->context()->parentSpanId);
        static::assertFalse($second->context()->traceId->equals($first->context()->traceId));
    }

    public function test_nested_spans_share_trace_id(): void
    {
        $tracer = TracerMother::create();
        $parent = $tracer->span('parent');
        $tracer->activate($parent);
        $child = $tracer->span('child');

        static::assertTrue($child->context()->traceId->equals($parent->context()->traceId));
    }

    public function test_span_calls_processor_on_start(): void
    {
        $processor = $this->createMock(SpanProcessor::class);
        $processor->expects(self::once())->method('onStart');

        TracerMother::withProcessor($processor)->span('test-span');
    }

    public function test_span_creates_span_with_given_kind(): void
    {
        static::assertSame(SpanKind::SERVER, TracerMother::create()->span('test-span', SpanKind::SERVER)->kind());
    }

    public function test_span_creates_span_with_given_name(): void
    {
        static::assertSame('my-operation', TracerMother::create()->span('my-operation')->name());
    }

    public function test_span_defaults_to_internal_kind(): void
    {
        static::assertSame(SpanKind::INTERNAL, TracerMother::create()->span('test-span')->kind());
    }

    public function test_span_inherits_context_active_span_as_parent(): void
    {
        $activeSpan = SpanContext::create(TraceId::generate(), SpanId::generate());
        $context = Context::root()->withActiveSpan($activeSpan);

        $span = TracerMother::withContext($context)->span('child');

        $parentSpanId = $span->context()->parentSpanId;
        static::assertNotNull($parentSpanId);
        static::assertTrue($parentSpanId->equals($activeSpan->spanId));
        static::assertTrue($span->context()->traceId->equals($activeSpan->traceId));
    }

    public function test_span_is_root_when_no_parent(): void
    {
        static::assertTrue(TracerMother::create()->span('root-span')->context()->isRoot());
    }

    public function test_root_span_generates_new_trace_id(): void
    {
        $span = TracerMother::create()->span('test-span');

        static::assertTrue($span->context()->traceId->isValid());
        static::assertTrue($span->context()->isRoot());
    }

    public function test_sequential_root_spans_get_distinct_trace_ids(): void
    {
        $tracer = TracerMother::create();

        $first = $tracer->span('first');
        $tracer->complete($first);
        $second = $tracer->span('second');
        $tracer->complete($second);

        static::assertFalse($first->context()->traceId->equals($second->context()->traceId));
    }

    public function test_parent_context_false_starts_a_new_trace_root(): void
    {
        $activeSpan = SpanContext::create(TraceId::generate(), SpanId::generate());
        $tracer = TracerMother::withContext(Context::root()->withActiveSpan($activeSpan));

        $span = $tracer->span('detached', SpanKind::INTERNAL, [], [], false);

        static::assertTrue($span->context()->isRoot());
        static::assertFalse($span->context()->traceId->equals($activeSpan->traceId));
    }

    public function test_trace_completes_span_after_callback(): void
    {
        $tracer = TracerMother::create();

        $tracer->trace('test-span', static function (): void {});

        static::assertNull($tracer->activeSpan());
    }

    public function test_trace_records_exception_on_error(): void
    {
        $processor = TracerMother::createMemoryProcessor();
        $tracer = TracerMother::withInMemoryProcessor($processor);

        try {
            $tracer->trace('test-span', static function (): void {
                throw new RuntimeException('Test error');
            });
        } catch (RuntimeException) {
        }

        static::assertCount(1, $processor->endedSpans());
        static::assertCount(1, $processor->endedSpans()[0]->events());
        static::assertSame('exception', $processor->endedSpans()[0]->events()[0]->name());
    }

    public function test_trace_rethrows_exception(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Test error');

        TracerMother::create()->trace('test-span', static function (): void {
            throw new RuntimeException('Test error');
        });
    }

    public function test_trace_returns_callback_result(): void
    {
        static::assertSame('success', TracerMother::create()->trace('test-span', static fn() => 'success'));
    }

    public function test_trace_sets_error_status_on_exception(): void
    {
        $processor = TracerMother::createMemoryProcessor();
        $tracer = TracerMother::withInMemoryProcessor($processor);

        try {
            $tracer->trace('test-span', static function (): void {
                throw new RuntimeException('Error');
            });
        } catch (RuntimeException) {
        }

        $span = $processor->endedSpans()[0];
        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame(RuntimeException::class, $span->attributes()['error.type']);
    }

    public function test_trace_leaves_status_unset_on_success(): void
    {
        $processor = TracerMother::createMemoryProcessor();
        $tracer = TracerMother::withInMemoryProcessor($processor);

        $tracer->trace('test-span', static fn() => 'ok');

        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($processor->endedSpans()[0]->status());
    }

    public function test_version_returns_tracer_version(): void
    {
        static::assertSame('1.0.0', TracerMother::create()->version());
    }

    public function test_span_is_non_recording_and_not_exported_when_context_is_suppressed(): void
    {
        $processor = TracerMother::createMemoryProcessor();
        $tracer = TracerMother::create(
            processor: $processor,
            contextStorage: new MemoryContextStorage(Context::root()->withSuppressedTracing()),
        );

        $span = $tracer->span('suppressed');

        static::assertFalse($span->isRecording());
        static::assertCount(0, $processor->startedSpans());

        $tracer->complete($span);

        static::assertCount(0, $processor->endedSpans());
    }

    public function test_nested_spans_stay_suppressed_within_a_suppressed_context(): void
    {
        $processor = TracerMother::createMemoryProcessor();
        $tracer = TracerMother::create(
            processor: $processor,
            contextStorage: new MemoryContextStorage(Context::root()->withSuppressedTracing()),
        );

        $parent = $tracer->span('parent');
        $child = $tracer->span('child');

        static::assertFalse($child->isRecording());

        $tracer->complete($child);
        $tracer->complete($parent);

        static::assertCount(0, $processor->endedSpans());
    }

    public function test_parent_based_always_off_root_drops_the_whole_trace(): void
    {
        $processor = TracerMother::createMemoryProcessor();
        $tracer = TracerMother::create(processor: $processor, sampler: parent_based_sampler(always_off_sampler()));

        $root = $tracer->span('root', SpanKind::SERVER);
        $scope = $tracer->activate($root);
        $child = $tracer->span('child', SpanKind::INTERNAL);

        $tracer->complete($child);
        $scope->detach();
        $tracer->complete($root);

        static::assertFalse($root->isRecording());
        static::assertFalse($child->isRecording());
        static::assertFalse($child->context()->traceFlags->isSampled());
        static::assertSame([], $processor->endedSpans());
    }

    public function test_parent_based_always_on_root_records_the_whole_trace(): void
    {
        $processor = TracerMother::createMemoryProcessor();
        $tracer = TracerMother::create(processor: $processor, sampler: parent_based_sampler(always_on_sampler()));

        $root = $tracer->span('root', SpanKind::SERVER);
        $scope = $tracer->activate($root);
        $child = $tracer->span('child', SpanKind::INTERNAL);

        $tracer->complete($child);
        $scope->detach();
        $tracer->complete($root);

        static::assertTrue($root->isRecording());
        static::assertTrue($child->isRecording());
        static::assertCount(2, $processor->endedSpans());
    }
}

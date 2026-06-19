<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextScope;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class ContextScopeTest extends TestCase
{
    public function test_attach_returns_scope_that_restores_context(): void
    {
        $storage = new MemoryContextStorage();

        $context1 = Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate()));
        $context2 = Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate()));
        $context3 = Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate()));

        $storage->attach($context1);
        static::assertSame($context1->activeSpan(), $storage->current()->activeSpan());

        $scope2 = $storage->attach($context2);
        static::assertSame($context2->activeSpan(), $storage->current()->activeSpan());

        $scope3 = $storage->attach($context3);
        static::assertSame($context3->activeSpan(), $storage->current()->activeSpan());

        $scope3->detach();
        static::assertSame($context2->activeSpan(), $storage->current()->activeSpan());

        $scope2->detach();
        static::assertSame($context1->activeSpan(), $storage->current()->activeSpan());
    }

    public function test_detach_is_idempotent(): void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $scope = $storage->attach(Context::root()->withActiveSpan(SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        )));

        static::assertSame(ContextScope::DETACHED, $scope->detach());
        static::assertSame(ContextScope::DETACHED, $scope->detach());
        static::assertSame(ContextScope::DETACHED, $scope->detach());

        static::assertSame($originalContext->activeSpan(), $storage->current()->activeSpan());
    }

    public function test_detach_restores_previous_context(): void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $newContext = Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate()));
        $scope = $storage->attach($newContext);

        static::assertSame($newContext->activeSpan(), $storage->current()->activeSpan());

        $scope->detach();

        static::assertSame($originalContext->activeSpan(), $storage->current()->activeSpan());
    }
}

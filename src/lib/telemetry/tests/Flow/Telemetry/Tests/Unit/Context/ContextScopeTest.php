<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\Scope;
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

    public function test_detach_returns_detached_when_in_order(): void
    {
        $storage = new MemoryContextStorage();

        $scope = $storage->attach(Context::root()->withActiveSpan(SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        )));

        static::assertSame(Scope::DETACHED, $scope->detach());
    }

    public function test_detach_returns_inactive_when_already_detached(): void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $scope = $storage->attach(Context::root()->withActiveSpan(SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        )));

        static::assertSame(Scope::DETACHED, $scope->detach());
        static::assertSame(Scope::INACTIVE, $scope->detach());
        static::assertSame(Scope::INACTIVE, $scope->detach());

        static::assertSame($originalContext->activeSpan(), $storage->current()->activeSpan());
    }

    public function test_detach_returns_mismatch_when_out_of_order(): void
    {
        $storage = new MemoryContextStorage();

        $scopeA = $storage->attach(Context::root()->withActiveSpan(SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        )));
        $storage->attach(Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate())));

        static::assertSame(Scope::MISMATCH, $scopeA->detach());
    }

    public function test_detach_out_of_order_still_restores_the_previous_context(): void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $scopeA = $storage->attach(Context::root()->withActiveSpan(SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
        )));
        $storage->attach(Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate())));

        $scopeA->detach();

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

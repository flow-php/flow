<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextScope;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\TraceId;
use PHPUnit\Framework\TestCase;

final class ContextScopeTest extends TestCase
{
    public function test_attach_returns_scope_that_restores_context(): void
    {
        $storage = new MemoryContextStorage();

        $context1 = Context::withTraceId(TraceId::generate());
        $context2 = Context::withTraceId(TraceId::generate());
        $context3 = Context::withTraceId(TraceId::generate());

        $storage->attach($context1);
        static::assertTrue($context1->traceId->equals($storage->current()->traceId));

        $scope2 = $storage->attach($context2);
        static::assertTrue($context2->traceId->equals($storage->current()->traceId));

        $scope3 = $storage->attach($context3);
        static::assertTrue($context3->traceId->equals($storage->current()->traceId));

        $scope3->detach();
        static::assertTrue($context2->traceId->equals($storage->current()->traceId));

        $scope2->detach();
        static::assertTrue($context1->traceId->equals($storage->current()->traceId));
    }

    public function test_detach_is_idempotent(): void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $newContext = Context::withTraceId(TraceId::generate());
        $scope = $storage->attach($newContext);

        static::assertSame(ContextScope::DETACHED, $scope->detach());
        static::assertSame(ContextScope::DETACHED, $scope->detach());
        static::assertSame(ContextScope::DETACHED, $scope->detach());

        static::assertTrue($originalContext->traceId->equals($storage->current()->traceId));
    }

    public function test_detach_restores_previous_context(): void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $newContext = Context::withTraceId(TraceId::generate());
        $scope = $storage->attach($newContext);

        static::assertTrue($newContext->traceId->equals($storage->current()->traceId));

        $scope->detach();

        static::assertTrue($originalContext->traceId->equals($storage->current()->traceId));
    }
}

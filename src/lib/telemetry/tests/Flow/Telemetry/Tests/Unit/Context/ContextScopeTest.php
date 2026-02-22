<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\{Context, ContextScope, MemoryContextStorage, TraceId};
use PHPUnit\Framework\TestCase;

final class ContextScopeTest extends TestCase
{
    public function test_attach_returns_scope_that_restores_context() : void
    {
        $storage = new MemoryContextStorage();

        $context1 = Context::withTraceId(TraceId::generate());
        $context2 = Context::withTraceId(TraceId::generate());
        $context3 = Context::withTraceId(TraceId::generate());

        $scope1 = $storage->attach($context1);
        self::assertTrue($context1->traceId->equals($storage->current()->traceId));

        $scope2 = $storage->attach($context2);
        self::assertTrue($context2->traceId->equals($storage->current()->traceId));

        $scope3 = $storage->attach($context3);
        self::assertTrue($context3->traceId->equals($storage->current()->traceId));

        $scope3->detach();
        self::assertTrue($context2->traceId->equals($storage->current()->traceId));

        $scope2->detach();
        self::assertTrue($context1->traceId->equals($storage->current()->traceId));
    }

    public function test_detach_is_idempotent() : void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $newContext = Context::withTraceId(TraceId::generate());
        $scope = $storage->attach($newContext);

        self::assertSame(ContextScope::DETACHED, $scope->detach());
        self::assertSame(ContextScope::DETACHED, $scope->detach());
        self::assertSame(ContextScope::DETACHED, $scope->detach());

        self::assertTrue($originalContext->traceId->equals($storage->current()->traceId));
    }

    public function test_detach_restores_previous_context() : void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $newContext = Context::withTraceId(TraceId::generate());
        $scope = $storage->attach($newContext);

        self::assertTrue($newContext->traceId->equals($storage->current()->traceId));

        $scope->detach();

        self::assertTrue($originalContext->traceId->equals($storage->current()->traceId));
    }
}

<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\Context\TraceId;
use PHPUnit\Framework\TestCase;

final class MemoryContextStorageTest extends TestCase
{
    public function test_attach_replaces_current_context(): void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $newContext = Context::withTraceId(TraceId::generate());
        $storage->attach($newContext);

        static::assertFalse($originalContext->traceId->equals($storage->current()->traceId));
        static::assertTrue($newContext->traceId->equals($storage->current()->traceId));
    }

    public function test_attach_returns_scope(): void
    {
        $storage = new MemoryContextStorage();
        $newContext = Context::withTraceId(TraceId::generate());

        $scope = $storage->attach($newContext);

        static::assertInstanceOf(Scope::class, $scope);
    }

    public function test_creates_default_context_when_none_provided(): void
    {
        $storage = new MemoryContextStorage();

        static::assertInstanceOf(Context::class, $storage->current());
    }

    public function test_current_returns_stored_context(): void
    {
        $context = Context::withTraceId(TraceId::generate());
        $storage = new MemoryContextStorage($context);

        static::assertSame($context->traceId->toHex(), $storage->current()->traceId->toHex());
    }

    public function test_implements_context_storage(): void
    {
        static::assertInstanceOf(ContextStorage::class, new MemoryContextStorage());
    }
}

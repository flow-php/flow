<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\ResettableContextStorage;
use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\TestCase;

final class MemoryContextStorageTest extends TestCase
{
    public function test_attach_replaces_current_context(): void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $newContext = Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate()));
        $storage->attach($newContext);

        static::assertNull($originalContext->activeSpan());
        static::assertSame($newContext->activeSpan(), $storage->current()->activeSpan());
    }

    public function test_attach_returns_scope(): void
    {
        $storage = new MemoryContextStorage();

        $scope = $storage->attach(Context::root());

        static::assertInstanceOf(Scope::class, $scope);
    }

    public function test_creates_default_root_context_when_none_provided(): void
    {
        $storage = new MemoryContextStorage();

        static::assertTrue($storage->current()->isRootContext());
    }

    public function test_current_returns_stored_context(): void
    {
        $context = Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate()));
        $storage = new MemoryContextStorage($context);

        static::assertSame($context->activeSpan(), $storage->current()->activeSpan());
    }

    public function test_implements_context_storage(): void
    {
        static::assertInstanceOf(ContextStorage::class, new MemoryContextStorage());
    }

    public function test_implements_resettable_context_storage(): void
    {
        static::assertInstanceOf(ResettableContextStorage::class, new MemoryContextStorage());
    }

    public function test_reset_returns_current_context_to_root(): void
    {
        $storage = new MemoryContextStorage();
        $storage->attach(Context::root()->withActiveSpan(SpanContext::create(TraceId::generate(), SpanId::generate())));

        static::assertFalse($storage->current()->isRootContext());

        $storage->reset();

        static::assertTrue($storage->current()->isRootContext());
    }
}

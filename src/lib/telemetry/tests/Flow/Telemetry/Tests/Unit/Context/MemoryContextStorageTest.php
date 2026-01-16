<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Context;

use Flow\Telemetry\Context\{Context, ContextStorage, MemoryContextStorage};
use PHPUnit\Framework\TestCase;

final class MemoryContextStorageTest extends TestCase
{
    public function test_creates_default_context_when_none_provided() : void
    {
        $storage = new MemoryContextStorage();

        self::assertInstanceOf(Context::class, $storage->current());
    }

    public function test_current_returns_stored_context() : void
    {
        $context = Context::create();
        $storage = new MemoryContextStorage($context);

        self::assertSame($context->traceId->toHex(), $storage->current()->traceId->toHex());
    }

    public function test_implements_context_storage() : void
    {
        self::assertInstanceOf(ContextStorage::class, new MemoryContextStorage());
    }

    public function test_store_replaces_current_context() : void
    {
        $storage = new MemoryContextStorage();
        $originalContext = $storage->current();

        $newContext = Context::create();
        $storage->store($newContext);

        self::assertFalse($originalContext->traceId->equals($storage->current()->traceId));
        self::assertTrue($newContext->traceId->equals($storage->current()->traceId));
    }

    public function test_store_updates_context_for_all_readers() : void
    {
        $storage = new MemoryContextStorage();
        $newContext = Context::create();

        $storage->store($newContext);

        self::assertSame($newContext->traceId->toHex(), $storage->current()->traceId->toHex());
    }
}

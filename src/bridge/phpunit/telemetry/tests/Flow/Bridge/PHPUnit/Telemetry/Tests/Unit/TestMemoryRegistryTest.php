<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit;

use Flow\Bridge\PHPUnit\Telemetry\TestMemoryRegistry;
use PHPUnit\Framework\TestCase;

final class TestMemoryRegistryTest extends TestCase
{
    public function test_clear_removes_stored_start(): void
    {
        $registry = new TestMemoryRegistry();
        $registry->setStart('test-id', 1024);

        $registry->clear('test-id');

        static::assertNull($registry->getStart('test-id'));
    }

    public function test_get_start_returns_null_for_unknown_test(): void
    {
        $registry = new TestMemoryRegistry();

        static::assertNull($registry->getStart('unknown'));
    }

    public function test_set_and_get_start(): void
    {
        $registry = new TestMemoryRegistry();
        $registry->setStart('test-id', 2048);

        static::assertSame(2048, $registry->getStart('test-id'));
    }
}

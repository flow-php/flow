<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Pipeline;

use Flow\Benchmarks\Pipeline\InMemoryOrders;
use PHPUnit\Framework\TestCase;

final class InMemoryOrdersTest extends TestCase
{
    public const ROWS = 100;

    public function test_every_row_is_keyed_by_column_name(): void
    {
        InMemoryOrders::clear();

        static::assertSame(
            [
                'order_id',
                'seller_id',
                'created_at',
                'updated_at',
                'cancelled_at',
                'discount',
                'email',
                'customer',
                'address',
                'notes',
                'items',
            ],
            array_keys(InMemoryOrders::of(self::ROWS)[0]),
        );
    }

    public function test_it_holds_the_requested_row_count(): void
    {
        InMemoryOrders::clear();

        static::assertCount(self::ROWS, InMemoryOrders::of(self::ROWS));
    }

    /**
     * The memo is what keeps a 426 MB array out of the timed window, so its absence would silently
     * turn 42% of the array/memory subjects into fixture construction.
     */
    public function test_it_memoises_per_row_count(): void
    {
        InMemoryOrders::clear();

        static::assertSame(InMemoryOrders::of(self::ROWS), InMemoryOrders::of(self::ROWS));
    }
}

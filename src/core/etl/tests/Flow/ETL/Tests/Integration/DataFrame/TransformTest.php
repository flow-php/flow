<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\with_entry;

final class TransformTest extends FlowTestCase
{
    public function test_transform_with_entries(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'status' => 'active', 'amount' => 41_000],
                ['id' => 2, 'status' => 'inactive', 'amount' => 250],
                ['id' => 3, 'status' => 'active', 'amount' => 1_000],
            ]))
            ->transform(with_entry('amount', ref('amount')->divide(100, scale: 2)))
            ->transform(with_entry('currency', lit('PLN')))
            ->fetch()
            ->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 410.00, 'currency' => 'PLN'],
                ['id' => 2, 'status' => 'inactive', 'amount' => 2.5, 'currency' => 'PLN'],
                ['id' => 3, 'status' => 'active', 'amount' => 10.00, 'currency' => 'PLN'],
            ],
            $rows,
        );
    }

    public function test_with_entries(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'status' => 'active', 'amount' => 41_000],
                ['id' => 2, 'status' => 'inactive', 'amount' => 250],
                ['id' => 3, 'status' => 'active', 'amount' => 1_000],
            ]))
            ->withEntries([
                'amount' => ref('amount')->divide(100, scale: 2),
                'currency' => lit('PLN'),
            ])
            ->fetch()
            ->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 410.00, 'currency' => 'PLN'],
                ['id' => 2, 'status' => 'inactive', 'amount' => 2.5, 'currency' => 'PLN'],
                ['id' => 3, 'status' => 'active', 'amount' => 10.00, 'currency' => 'PLN'],
            ],
            $rows,
        );
    }

    public function test_with_entries_object_oriented(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'status' => 'active', 'amount' => 41_000],
                ['id' => 2, 'status' => 'inactive', 'amount' => 250],
                ['id' => 3, 'status' => 'active', 'amount' => 1_000],
            ]))
            ->withEntries([
                with_entry('amount', ref('amount')->divide(100, scale: 2)),
                with_entry('currency', lit('PLN')),
            ])
            ->fetch()
            ->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 410.00, 'currency' => 'PLN'],
                ['id' => 2, 'status' => 'inactive', 'amount' => 2.5, 'currency' => 'PLN'],
                ['id' => 3, 'status' => 'active', 'amount' => 10.00, 'currency' => 'PLN'],
            ],
            $rows,
        );
    }

    public function test_with_entry(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'status' => 'active', 'amount' => 41_000],
                ['id' => 2, 'status' => 'inactive', 'amount' => 250],
                ['id' => 3, 'status' => 'active', 'amount' => 1_000],
            ]))
            ->withEntry('amount', ref('amount')->divide(100, scale: 2))
            ->withEntry('currency', lit('PLN'))
            ->fetch()
            ->toArray();

        static::assertEqualsWithDelta(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 410.00, 'currency' => 'PLN'],
                ['id' => 2, 'status' => 'inactive', 'amount' => 2.5, 'currency' => 'PLN'],
                ['id' => 3, 'status' => 'active', 'amount' => 10.00, 'currency' => 'PLN'],
            ],
            $rows,
            0.01,
        );
    }

    public function test_with_with_entries(): void
    {
        $rows = df()
            ->read(from_array([
                ['id' => 1, 'status' => 'active', 'amount' => 41_000],
                ['id' => 2, 'status' => 'inactive', 'amount' => 250],
                ['id' => 3, 'status' => 'active', 'amount' => 1_000],
            ]))
            ->with(with_entry('amount', ref('amount')->divide(100, scale: 2)))
            ->with(with_entry('currency', lit('PLN')))
            ->fetch()
            ->toArray();

        static::assertEquals(
            [
                ['id' => 1, 'status' => 'active', 'amount' => 410.00, 'currency' => 'PLN'],
                ['id' => 2, 'status' => 'inactive', 'amount' => 2.5, 'currency' => 'PLN'],
                ['id' => 3, 'status' => 'active', 'amount' => 10.00, 'currency' => 'PLN'],
            ],
            $rows,
        );
    }
}

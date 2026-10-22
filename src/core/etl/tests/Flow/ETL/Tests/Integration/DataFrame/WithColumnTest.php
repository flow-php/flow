<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\with_column;

final class WithColumnTest extends FlowTestCase
{
    public function test_deprecated_with_entries_delegates_to_with_columns(): void
    {
        static::assertSame(
            [['a' => 1, 'c' => 1, 'd' => 2]],
            df()
                ->read(from_array([['a' => 1]]))
                // @mago-expect analysis:deprecated-method
                ->withEntries(['c' => lit(1), with_column('d', lit(2))])
                ->fetch()
                ->toArray(),
        );
    }

    public function test_deprecated_with_entry_delegates_to_with_column(): void
    {
        static::assertSame(
            [['a' => 1, 'b' => 2, 'c' => 3]],
            df()
                ->read(from_array([['a' => 1, 'b' => 2]]))
                // @mago-expect analysis:deprecated-method
                ->withEntry('c', ref('a')->plus(ref('b')))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_with_column_adds_column(): void
    {
        static::assertSame(
            [['a' => 1, 'b' => 2, 'c' => 3]],
            df()
                ->read(from_array([['a' => 1, 'b' => 2]]))
                ->withColumn('c', ref('a')->plus(ref('b')))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_with_columns_accepts_named_functions_and_with_column(): void
    {
        static::assertSame(
            [['a' => 1, 'c' => 1, 'd' => 2]],
            df()
                ->read(from_array([['a' => 1]]))
                ->withColumns(['c' => lit(1), with_column('d', lit(2))])
                ->fetch()
                ->toArray(),
        );
    }
}

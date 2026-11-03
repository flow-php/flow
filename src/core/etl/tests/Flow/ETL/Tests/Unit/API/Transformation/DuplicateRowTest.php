<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\duplicate_row;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\with_column;

final class DuplicateRowTest extends FlowTestCase
{
    public function test_duplicate_row_through_invoke(): void
    {
        static::assertSame(
            [['a' => 1], ['a' => 10], ['a' => 2]],
            duplicate_row(ref('a')->equals(lit(1)), with_column('a', lit(10)))(df()->read(from_array([
                ['a' => 1],
                ['a' => 2],
            ])))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_duplicate_row_through_with(): void
    {
        static::assertSame(
            [['a' => 1], ['a' => 10], ['a' => 2]],
            df()
                ->read(from_array([['a' => 1], ['a' => 2]]))
                ->with(duplicate_row(ref('a')->equals(lit(1)), with_column('a', lit(10))))
                ->fetch()
                ->toArray(),
        );
    }
}

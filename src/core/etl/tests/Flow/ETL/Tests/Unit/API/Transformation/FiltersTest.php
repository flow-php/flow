<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\filters;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class FiltersTest extends FlowTestCase
{
    public function test_filters_through_invoke(): void
    {
        static::assertSame(
            [['a' => 2]],
            filters([ref('a')->greaterThan(lit(1)), ref('a')->lessThan(lit(3))])(df()->read(from_array([
                ['a' => 1],
                ['a' => 2],
                ['a' => 3],
            ])))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_filters_through_with(): void
    {
        static::assertSame(
            [['a' => 2]],
            df()
                ->read(from_array([['a' => 1], ['a' => 2], ['a' => 3]]))
                ->with(filters([ref('a')->greaterThan(lit(1)), ref('a')->lessThan(lit(3))]))
                ->fetch()
                ->toArray(),
        );
    }
}

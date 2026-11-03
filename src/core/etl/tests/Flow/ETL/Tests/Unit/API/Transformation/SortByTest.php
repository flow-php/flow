<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sort_by;

final class SortByTest extends FlowTestCase
{
    public function test_sort_by_through_invoke(): void
    {
        static::assertSame(
            [['a' => 3], ['a' => 2], ['a' => 1]],
            sort_by(ref('a')->desc())(df()->read(from_array([['a' => 1], ['a' => 3], ['a' => 2]])))->fetch()->toArray(),
        );
    }

    public function test_sort_by_through_with(): void
    {
        static::assertSame(
            [['a' => 3], ['a' => 2], ['a' => 1]],
            df()
                ->read(from_array([['a' => 1], ['a' => 3], ['a' => 2]]))
                ->with(sort_by(ref('a')->desc()))
                ->fetch()
                ->toArray(),
        );
    }
}

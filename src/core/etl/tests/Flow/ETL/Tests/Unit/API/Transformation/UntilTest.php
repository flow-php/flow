<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\until;

final class UntilTest extends FlowTestCase
{
    public function test_until_through_invoke(): void
    {
        static::assertSame(
            [['a' => 1], ['a' => 2]],
            until(ref('a')->lessThan(lit(3)))(df()->read(from_array([['a' => 1], ['a' => 2], ['a' => 3], ['a' => 1]])))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_until_through_with(): void
    {
        static::assertSame(
            [['a' => 1], ['a' => 2]],
            df()
                ->read(from_array([['a' => 1], ['a' => 2], ['a' => 3], ['a' => 1]]))
                ->with(until(ref('a')->lessThan(lit(3))))
                ->fetch()
                ->toArray(),
        );
    }
}

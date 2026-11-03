<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\with;

final class WithTest extends FlowTestCase
{
    public function test_with_through_invoke(): void
    {
        static::assertSame(
            [['a' => 1]],
            with(select('a'))(df()->read(from_array([['a' => 1, 'b' => 2]])))->fetch()->toArray(),
        );
    }

    public function test_with_through_with(): void
    {
        static::assertSame(
            [['a' => 1]],
            df()
                ->read(from_array([['a' => 1, 'b' => 2]]))
                ->with(with(select('a')))
                ->fetch()
                ->toArray(),
        );
    }
}

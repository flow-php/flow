<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\with_columns;

final class WithColumnsTest extends FlowTestCase
{
    public function test_with_columns_through_invoke(): void
    {
        static::assertSame(
            [['a' => 1, 'b' => 2]],
            with_columns(['b' => lit(2)])(df()->read(from_array([['a' => 1]])))->fetch()->toArray(),
        );
    }

    public function test_with_columns_through_with(): void
    {
        static::assertSame(
            [['a' => 1, 'b' => 2]],
            df()
                ->read(from_array([['a' => 1]]))
                ->with(with_columns(['b' => lit(2)]))
                ->fetch()
                ->toArray(),
        );
    }
}

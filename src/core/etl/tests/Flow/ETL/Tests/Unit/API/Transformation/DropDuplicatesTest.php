<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\drop_duplicates;
use function Flow\ETL\DSL\from_array;

final class DropDuplicatesTest extends FlowTestCase
{
    public function test_drop_duplicates_through_invoke(): void
    {
        static::assertSame(
            [['a' => 1], ['a' => 2]],
            drop_duplicates('a')(df()->read(from_array([['a' => 1], ['a' => 1], ['a' => 2]])))->fetch()->toArray(),
        );
    }

    public function test_drop_duplicates_through_with(): void
    {
        static::assertSame(
            [['a' => 1], ['a' => 2]],
            df()
                ->read(from_array([['a' => 1], ['a' => 1], ['a' => 2]]))
                ->with(drop_duplicates('a'))
                ->fetch()
                ->toArray(),
        );
    }
}

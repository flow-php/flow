<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\offset;

final class OffsetTest extends FlowTestCase
{
    public function test_offset_through_invoke(): void
    {
        static::assertSame(
            [['a' => 2], ['a' => 3]],
            offset(1)(df()->read(from_array([['a' => 1], ['a' => 2], ['a' => 3]])))->fetch()->toArray(),
        );
    }

    public function test_offset_through_with(): void
    {
        static::assertSame(
            [['a' => 2], ['a' => 3]],
            df()
                ->read(from_array([['a' => 1], ['a' => 2], ['a' => 3]]))
                ->with(offset(1))
                ->fetch()
                ->toArray(),
        );
    }
}

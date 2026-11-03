<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\rename_each;
use function Flow\ETL\DSL\rename_replace;

final class RenameEachTest extends FlowTestCase
{
    public function test_rename_each_through_invoke(): void
    {
        static::assertSame(
            [['a-x' => 1]],
            rename_each(rename_replace('_', '-'))(df()->read(from_array([['a_x' => 1]])))->fetch()->toArray(),
        );
    }

    public function test_rename_each_through_with(): void
    {
        static::assertSame(
            [['a-x' => 1]],
            df()
                ->read(from_array([['a_x' => 1]]))
                ->with(rename_each(rename_replace('_', '-')))
                ->fetch()
                ->toArray(),
        );
    }
}

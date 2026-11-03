<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\rename;

final class RenameTest extends FlowTestCase
{
    public function test_rename_through_invoke(): void
    {
        static::assertSame([['b' => 1]], rename('a', 'b')(df()->read(from_array([['a' => 1]])))->fetch()->toArray());
    }

    public function test_rename_through_with(): void
    {
        static::assertSame(
            [['b' => 1]],
            df()
                ->read(from_array([['a' => 1]]))
                ->with(rename('a', 'b'))
                ->fetch()
                ->toArray(),
        );
    }
}

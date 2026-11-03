<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\void;

final class DiscardTest extends FlowTestCase
{
    public function test_void_through_invoke(): void
    {
        static::assertSame(
            [],
            void()(df()->read(from_array([['a' => 1], ['a' => 2], ['a' => 3]])))->fetch()->toArray(),
        );
    }

    public function test_void_through_with(): void
    {
        static::assertSame(
            [],
            df()
                ->read(from_array([['a' => 1], ['a' => 2], ['a' => 3]]))
                ->with(void())
                ->fetch()
                ->toArray(),
        );
    }
}

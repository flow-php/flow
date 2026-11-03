<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\collect_refs;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\refs;

final class CollectRefsTest extends FlowTestCase
{
    public function test_collect_refs_through_invoke(): void
    {
        collect_refs($refs = refs())(df()->read(from_array([['a' => 1, 'b' => 2]])))->run();

        static::assertEquals(refs('a', 'b'), $refs);
    }

    public function test_collect_refs_through_with(): void
    {
        df()
            ->read(from_array([['a' => 1, 'b' => 2]]))
            ->with(collect_refs($refs = refs()))
            ->run();

        static::assertEquals(refs('a', 'b'), $refs);
    }
}

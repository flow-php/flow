<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\HashJoin;

use Flow\ETL\Kernels\HashJoin\JoinSide;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class JoinSideTest extends FlowTestCase
{
    public function test_the_null_row_follows_the_side_schema_and_is_built_once(): void
    {
        $side = JoinSide::of(GroupByContext::batches(), schema(int_schema('id')));
        $nullRow = $side->nullRow(schema(str_schema('ignored')), new PhpBackend());

        static::assertSame([['id' => null]], $nullRow->toArray());
        static::assertSame($nullRow, $side->nullRow(schema(str_schema('ignored')), new PhpBackend()));
    }

    public function test_a_side_without_a_schema_takes_the_fallback(): void
    {
        static::assertSame(
            [['name' => null]],
            JoinSide::of(GroupByContext::batches())->nullRow(schema(str_schema('name')), new PhpBackend())->toArray(),
        );
    }
}

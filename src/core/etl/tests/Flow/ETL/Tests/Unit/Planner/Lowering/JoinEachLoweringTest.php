<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Plan\Node\JoinEach;
use Flow\ETL\Planner\Lowering\JoinEachLowering;
use Flow\ETL\Tests\Double\StaticDataFrameFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\join_on;

final class JoinEachLoweringTest extends FlowTestCase
{
    #[TestWith([JoinType::left])]
    #[TestWith([JoinType::left_anti])]
    #[TestWith([JoinType::right])]
    #[TestWith([JoinType::inner])]
    public function test_the_named_constructor_follows_the_join_type(JoinType $type): void
    {
        $factory = new StaticDataFrameFactory(df()->read(from_array([['id' => 1]])));
        $on = join_on(['id' => 'id']);

        static::assertEquals(
            [match ($type) {
                JoinType::left => JoinEachRowsTransformer::left($factory, $on),
                JoinType::left_anti => JoinEachRowsTransformer::leftAnti($factory, $on),
                JoinType::right => JoinEachRowsTransformer::right($factory, $on),
                JoinType::inner => JoinEachRowsTransformer::inner($factory, $on),
            }],
            (new JoinEachLowering())->steps(
                new JoinEach(NodeMother::read(), $factory, $on, $type),
                NodeMother::context(),
                [],
            ),
        );
    }
}

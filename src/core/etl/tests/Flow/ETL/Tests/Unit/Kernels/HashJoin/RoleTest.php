<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\HashJoin;

use Flow\ETL\Kernels\HashJoin\Role;
use Flow\ETL\Kernels\HashJoin\RowMerger;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Join\Side;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class RoleTest extends FlowTestCase
{
    #[TestWith([JoinType::inner, false, false])]
    #[TestWith([JoinType::left, true, false])]
    #[TestWith([JoinType::left_anti, true, false])]
    #[TestWith([JoinType::right, false, true])]
    public function test_the_built_side_tracks_its_unmatched_rows_when_the_join_keeps_them(
        JoinType $type,
        bool $buildLeft,
        bool $buildRight,
    ): void {
        static::assertSame(Side::left, Role::buildLeft($type)->built());
        static::assertSame(Side::right, Role::buildRight($type)->built());
        static::assertSame($buildLeft, Role::buildLeft($type)->tracksUnmatched());
        static::assertSame($buildRight, Role::buildRight($type)->tracksUnmatched());
    }

    public function test_the_merge_keeps_left_then_right_whatever_was_built(): void
    {
        $merger = new RowMerger(schema(int_schema('l'), int_schema('r')), ['l'], ['r']);
        $left = array_to_rows([['l' => 1]], schema(int_schema('l')));
        $right = array_to_rows([['r' => 2]], schema(int_schema('r')));

        static::assertSame(
            [['l' => 1, 'r' => 2]],
            Role::buildRight(JoinType::inner)->merge($merger, $left, $right)->toArray(),
        );
        static::assertSame(
            [['l' => 1, 'r' => 2]],
            Role::buildLeft(JoinType::inner)->merge($merger, $right, $left)->toArray(),
        );
    }
}

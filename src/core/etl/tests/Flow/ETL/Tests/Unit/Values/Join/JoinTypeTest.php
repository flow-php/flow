<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Join;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Join\Side;
use PHPUnit\Framework\Attributes\TestWith;

final class JoinTypeTest extends FlowTestCase
{
    #[TestWith([JoinType::inner, true, true])]
    #[TestWith([JoinType::left, false, true])]
    #[TestWith([JoinType::left_anti, false, true])]
    #[TestWith([JoinType::right, true, false])]
    public function test_a_side_whose_unmatched_rows_are_not_kept_drops_its_null_keys(
        JoinType $type,
        bool $left,
        bool $right,
    ): void {
        static::assertSame($left, $type->dropsNullKeys(Side::left));
        static::assertSame($right, $type->dropsNullKeys(Side::right));
    }

    #[TestWith([JoinType::inner, false, false])]
    #[TestWith([JoinType::left, true, false])]
    #[TestWith([JoinType::left_anti, false, false])]
    #[TestWith([JoinType::right, false, true])]
    public function test_only_an_outer_side_pads_its_unmatched_rows_with_a_null_row(
        JoinType $type,
        bool $left,
        bool $right,
    ): void {
        static::assertSame($left, $type->padsWithNullRow(Side::left));
        static::assertSame($right, $type->padsWithNullRow(Side::right));
    }
}

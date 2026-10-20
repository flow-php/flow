<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Plan\Redefined;
use Flow\ETL\Tests\FlowTestCase;

final class RedefinedTest extends FlowTestCase
{
    public function test_none_defines_nothing(): void
    {
        static::assertFalse(Redefined::none()->defines('year'));
        static::assertSame([], Redefined::none()->names);
        static::assertFalse(Redefined::none()->isUnknown());
        static::assertFalse(Redefined::none()->isAllBut());
    }

    public function test_names_define_exactly_those_names_and_alias_none(): void
    {
        $redefined = Redefined::names('year', 'month');

        static::assertTrue($redefined->defines('month'));
        static::assertFalse($redefined->defines('day'));
        static::assertNull($redefined->aliasOf('month'));
        static::assertSame(['year', 'month'], $redefined->names);
    }

    public function test_unknown_defines_every_name_and_aliases_none(): void
    {
        static::assertTrue(Redefined::unknown()->defines('anything'));
        static::assertNull(Redefined::unknown()->aliasOf('anything'));
        static::assertTrue(Redefined::unknown()->isUnknown());
        static::assertFalse(Redefined::unknown()->isAllBut());
    }

    public function test_names_with_no_names_defines_nothing(): void
    {
        static::assertFalse(Redefined::names()->defines('year'));
        static::assertSame([], Redefined::names()->names);
    }

    public function test_an_alias_defines_its_name_and_points_it_at_the_column_below(): void
    {
        $redefined = Redefined::alias('y', 'year');

        static::assertTrue($redefined->defines('y'));
        static::assertFalse($redefined->defines('year'));
        static::assertSame('year', $redefined->aliasOf('y'));
        static::assertNull($redefined->aliasOf('year'));
        static::assertSame(['y'], $redefined->names);
    }

    public function test_all_but_defines_every_name_except_the_kept_ones(): void
    {
        $redefined = Redefined::allBut('year');

        static::assertFalse($redefined->defines('year'));
        static::assertTrue($redefined->defines('month'));
        static::assertNull($redefined->aliasOf('month'));
        static::assertSame(['year'], $redefined->names);
        static::assertTrue($redefined->isAllBut());
        static::assertFalse($redefined->isUnknown());
    }

    public function test_a_rename_defines_both_names_and_points_the_new_one_at_the_old_one(): void
    {
        $redefined = Redefined::rename('year', 'y');

        static::assertTrue($redefined->defines('y'));
        static::assertTrue($redefined->defines('year'));
        static::assertSame('year', $redefined->aliasOf('y'));
        static::assertNull($redefined->aliasOf('year'));
        static::assertSame(['y', 'year'], $redefined->names);
    }
}

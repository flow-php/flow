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
        static::assertFalse(Redefined::none()->unknown);
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
        static::assertTrue(Redefined::unknown()->unknown);
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
}

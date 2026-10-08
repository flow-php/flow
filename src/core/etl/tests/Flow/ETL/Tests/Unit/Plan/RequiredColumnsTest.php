<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Tests\FlowTestCase;

final class RequiredColumnsTest extends FlowTestCase
{
    public function test_all_requires_every_column(): void
    {
        static::assertTrue(RequiredColumns::all()->isAll());
        static::assertTrue(RequiredColumns::all()->requires('anything'));
    }

    public function test_all_but_requires_every_column_except_the_named_ones(): void
    {
        $required = RequiredColumns::allBut('body');

        static::assertFalse($required->isAll());
        static::assertFalse($required->requires('body'));
        static::assertTrue($required->requires('id'));
    }

    public function test_all_but_nothing_is_all(): void
    {
        static::assertTrue(RequiredColumns::allBut()->isAll());
    }

    public function test_only_requires_exactly_the_named_columns(): void
    {
        $required = RequiredColumns::only('record');

        static::assertFalse($required->isAll());
        static::assertTrue($required->requires('record'));
        static::assertFalse($required->requires('body'));
    }

    public function test_only_nothing_requires_no_column(): void
    {
        static::assertFalse(RequiredColumns::only()->isAll());
        static::assertFalse(RequiredColumns::only()->requires('id'));
    }

    public function test_duplicate_names_collapse(): void
    {
        static::assertSame(['a', 'b'], RequiredColumns::only('a', 'b', 'a')->names);
        static::assertSame(['a'], RequiredColumns::allBut('a', 'a')->names);
    }

    public function test_with_adds_to_only_and_removes_from_all_but(): void
    {
        static::assertEquals(RequiredColumns::only('a', 'b'), RequiredColumns::only('a')->with('b', 'a'));
        static::assertEquals(RequiredColumns::allBut('c'), RequiredColumns::allBut('b', 'c')->with('b'));
    }

    public function test_without_removes_from_only_and_adds_to_all_but(): void
    {
        static::assertEquals(RequiredColumns::only('b'), RequiredColumns::only('a', 'b')->without('a', 'x'));
        static::assertEquals(RequiredColumns::allBut('a', 'b'), RequiredColumns::allBut('a')->without('b', 'a'));
    }

    public function test_union_of_two_only_is_only_their_union(): void
    {
        static::assertEquals(
            RequiredColumns::only('a', 'b', 'c'),
            RequiredColumns::only('a', 'b')->union(RequiredColumns::only('b', 'c')),
        );
    }

    public function test_union_of_only_and_all_but_is_all_but_what_neither_reads(): void
    {
        static::assertEquals(
            RequiredColumns::allBut('y'),
            RequiredColumns::only('x')->union(RequiredColumns::allBut('x', 'y')),
        );
        static::assertEquals(
            RequiredColumns::allBut('y'),
            RequiredColumns::allBut('x', 'y')->union(RequiredColumns::only('x')),
        );
    }

    public function test_union_of_two_all_but_excludes_only_what_both_exclude(): void
    {
        static::assertEquals(
            RequiredColumns::allBut('b'),
            RequiredColumns::allBut('a', 'b')->union(RequiredColumns::allBut('b', 'c')),
        );
    }
}

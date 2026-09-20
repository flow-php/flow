<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Plan\Explain\Condition;
use Flow\ETL\Tests\Fixtures\Join\AlwaysMeets;
use Flow\ETL\Tests\FlowTestCase;

final class ConditionTest extends FlowTestCase
{
    public function test_a_comparison_reads_as_its_two_sides_around_its_operator(): void
    {
        static::assertSame('id = user_id', (new Condition())->of(new Equal('id', 'user_id')));
        static::assertSame('id === user_id', (new Condition())->of(new Identical('id', 'user_id')));
    }

    public function test_an_and_is_one_line_per_comparison_and_anything_else_stays_one_line(): void
    {
        $condition = new Condition();

        static::assertSame(
            ['id = id', 'tag = tag'],
            $condition->lines(new All(new Equal('id', 'id'), new Equal('tag', 'tag'))),
        );
        static::assertSame(
            ['id = id OR tag = tag'],
            $condition->lines(new Any(new Equal('id', 'id'), new Equal('tag', 'tag'))),
        );
        static::assertSame(['id = id'], $condition->lines(new Equal('id', 'id')));
    }

    public function test_nested_comparisons_keep_their_operators(): void
    {
        static::assertSame(
            'id = id AND (name = name OR tag === tag)',
            (new Condition())->of(
                new All(new Equal('id', 'id'), new Any(new Equal('name', 'name'), new Identical('tag', 'tag'))),
            ),
        );
    }

    public function test_a_comparison_the_explain_does_not_know_reads_as_its_name(): void
    {
        static::assertSame('AlwaysMeets', (new Condition())->of(new AlwaysMeets()));
    }
}

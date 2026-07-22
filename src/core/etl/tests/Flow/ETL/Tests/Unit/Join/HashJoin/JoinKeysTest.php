<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Join\HashJoin\JoinKeys;
use Flow\ETL\Row\Reference;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;

final class JoinKeysTest extends FlowTestCase
{
    public function test_all_of_equal_comparisons_collect_all_pairs(): void
    {
        $keys = JoinKeys::fromComparison(new All(new Equal('id', 'user_id'), new Equal('country', 'country_code')));

        static::assertNotNull($keys);
        static::assertSame(
            ['id', 'country'],
            array_map(static fn(Reference $ref): string => $ref->name(), $keys->leftRefs()),
        );
        static::assertSame(
            ['user_id', 'country_code'],
            array_map(static fn(Reference $ref): string => $ref->name(), $keys->rightRefs()),
        );
    }

    public function test_any_comparison_is_not_equality_based(): void
    {
        static::assertNull(JoinKeys::fromComparison(new Any(new Equal('id', 'id'), new Equal('email', 'email'))));
    }

    public function test_all_with_any_extracts_the_equalities(): void
    {
        $keys = JoinKeys::fromComparison(
            new All(new Equal('id', 'user_id'), new Any(new Equal('a', 'a'), new Equal('b', 'b'))),
        );

        static::assertNotNull($keys);
        static::assertSame(['id'], array_map(static fn(Reference $ref): string => $ref->name(), $keys->leftRefs()));
        static::assertSame(
            ['user_id'],
            array_map(static fn(Reference $ref): string => $ref->name(), $keys->rightRefs()),
        );
    }

    public function test_all_of_only_non_equalities_is_not_extractable(): void
    {
        static::assertNull(JoinKeys::fromComparison(
            new All(
                new Any(new Equal('a', 'a'), new Equal('b', 'b')),
                new Any(new Equal('c', 'c'), new Equal('d', 'd')),
            ),
        ));
    }

    public function test_nested_all_equalities_are_extracted_next_to_an_any(): void
    {
        $keys = JoinKeys::fromComparison(
            new All(
                new All(new Equal('id', 'user_id'), new Equal('country', 'country_code')),
                new Any(new Equal('a', 'a'), new Equal('b', 'b')),
            ),
        );

        static::assertNotNull($keys);
        static::assertSame(
            ['id', 'country'],
            array_map(static fn(Reference $ref): string => $ref->name(), $keys->leftRefs()),
        );
    }

    public function test_duplicated_pairs_are_collected_once(): void
    {
        $keys = JoinKeys::fromComparison(new All(new Equal('id', 'user_id'), new Equal('id', 'user_id')));

        static::assertNotNull($keys);
        static::assertCount(1, $keys->leftRefs());
        static::assertCount(1, $keys->rightRefs());
    }

    public function test_duplicated_left_names_with_different_right_names_are_kept(): void
    {
        $keys = JoinKeys::fromComparison(new All(new Equal('id', 'user_id'), new Equal('id', 'account_id')));

        static::assertNotNull($keys);
        static::assertSame(
            ['id', 'id'],
            array_map(static fn(Reference $ref): string => $ref->name(), $keys->leftRefs()),
        );
        static::assertSame(
            ['user_id', 'account_id'],
            array_map(static fn(Reference $ref): string => $ref->name(), $keys->rightRefs()),
        );
    }

    public function test_identical_comparison_is_equality_based(): void
    {
        $keys = JoinKeys::fromComparison(new Identical('id', 'user_id'));

        static::assertNotNull($keys);
        static::assertSame('id', $keys->leftRefs()[0]->name());
        static::assertSame('user_id', $keys->rightRefs()[0]->name());
    }
}

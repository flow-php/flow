<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Join\Comparison\All;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Join\HashJoin\EqualityJoinKeys;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\uuid_entry;

final class EqualityJoinKeysTest extends FlowTestCase
{
    public function test_all_of_equal_comparisons_hash_all_pairs(): void
    {
        $keys = EqualityJoinKeys::fromComparison(
            new All(new Equal('id', 'user_id'), new Equal('country', 'country_code')),
        );

        static::assertNotNull($keys);
        static::assertSame(
            $keys->leftHash(row(int_entry('id', 1), str_entry('country', 'PL'))),
            $keys->rightHash(row(int_entry('user_id', 1), str_entry('country_code', 'PL'))),
        );
        static::assertNotSame(
            $keys->leftHash(row(int_entry('id', 1), str_entry('country', 'PL'))),
            $keys->rightHash(row(int_entry('user_id', 1), str_entry('country_code', 'US'))),
        );
    }

    public function test_any_comparison_is_not_equality_based(): void
    {
        static::assertNull(EqualityJoinKeys::fromComparison(
            new Any(new Equal('id', 'id'), new Equal('email', 'email')),
        ));
        static::assertNull(EqualityJoinKeys::fromComparison(
            new All(new Equal('id', 'id'), new Any(new Equal('a', 'a'), new Equal('b', 'b'))),
        ));
    }

    public function test_boolean_does_not_collide_with_numeric(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('flag', 'flag'));

        static::assertNotNull($keys);
        static::assertNotSame(
            $keys->leftHash(row(bool_entry('flag', true))),
            $keys->rightHash(row(int_entry('flag', 1))),
        );
    }

    public function test_datetime_instants_hash_equal_across_timezones(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('date', 'date'));

        static::assertNotNull($keys);
        static::assertSame(
            $keys->leftHash(row(datetime_entry(
                'date',
                new DateTimeImmutable('2024-01-01 12:00:00', new DateTimeZone('UTC')),
            ))),
            $keys->rightHash(row(datetime_entry(
                'date',
                new DateTimeImmutable('2024-01-01 13:00:00', new DateTimeZone('+01:00')),
            ))),
        );
    }

    public function test_different_values_hash_differently(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'user_id'));

        static::assertNotNull($keys);
        static::assertNotSame($keys->leftHash(row(int_entry('id', 1))), $keys->rightHash(row(int_entry('user_id', 2))));
    }

    public function test_identical_comparison_is_equality_based(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Identical('id', 'user_id'));

        static::assertNotNull($keys);
        static::assertSame($keys->leftHash(row(int_entry('id', 1))), $keys->rightHash(row(int_entry('user_id', 1))));
    }

    public function test_left_and_right_hashes_use_their_own_references(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'user_id'));

        static::assertNotNull($keys);
        static::assertSame(
            $keys->leftHash(row(int_entry('id', 5), str_entry('noise', 'left'))),
            $keys->rightHash(row(int_entry('user_id', 5), str_entry('other_noise', 'right'))),
        );
    }

    public function test_normalize_collapses_negative_zero(): void
    {
        static::assertSame(EqualityJoinKeys::normalize(-0.0), EqualityJoinKeys::normalize(0.0));
    }

    public function test_null_values_hash_equal(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'user_id'));

        static::assertNotNull($keys);
        static::assertSame(
            $keys->leftHash(row(int_entry('id', null))),
            $keys->rightHash(row(int_entry('user_id', null))),
        );
    }

    public function test_numeric_values_hash_equal_across_types(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'user_id'));

        static::assertNotNull($keys);
        static::assertSame(
            $keys->leftHash(row(int_entry('id', 1))),
            $keys->rightHash(row(float_entry('user_id', 1.0))),
        );
        static::assertSame($keys->leftHash(row(int_entry('id', 1))), $keys->rightHash(row(str_entry('user_id', '1'))));
    }

    public function test_uuid_objects_hash_by_their_string_representation(): void
    {
        $keys = EqualityJoinKeys::fromComparison(new Equal('id', 'id'));

        static::assertNotNull($keys);
        static::assertSame(
            $keys->leftHash(row(uuid_entry('id', 'f47ac10b-58cc-4372-a567-0e02b2c3d479'))),
            $keys->rightHash(row(uuid_entry('id', 'f47ac10b-58cc-4372-a567-0e02b2c3d479'))),
        );
        static::assertNotSame(
            $keys->leftHash(row(uuid_entry('id', 'f47ac10b-58cc-4372-a567-0e02b2c3d479'))),
            $keys->rightHash(row(uuid_entry('id', '00000000-0000-4000-8000-000000000000'))),
        );
    }
}

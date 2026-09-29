<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use DateTimeImmutable;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class EqualTest extends FlowTestCase
{
    public function test_failure(): void
    {
        static::assertSame(
            [false],
            (new Equal('datetime', 'datetime'))->compare(
                array_to_rows([[
                    'datetime' => new DateTimeImmutable('2022-10-01 00:00:00'),
                ]], schema(datetime_schema('datetime'))),
                array_to_rows([[
                    'datetime' => new DateTimeImmutable('2022-10-01 01:00:00'),
                ]], schema(datetime_schema('datetime'))),
            ),
        );
    }

    public function test_null_is_not_equal_to_null(): void
    {
        static::assertSame(
            [false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => null]], schema(str_schema('id', nullable: true))),
                array_to_rows([['id' => null]], schema(str_schema('id', nullable: true))),
            ),
        );
    }

    public function test_null_is_not_equal_to_value(): void
    {
        static::assertSame(
            [false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => null]], schema(int_schema('id', nullable: true))),
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
            ),
        );
        static::assertSame(
            [false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => null]], schema(int_schema('id', nullable: true))),
            ),
        );
    }

    public function test_object_and_scalar_are_not_equal(): void
    {
        static::assertSame(
            [false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']], schema(str_schema('id'))),
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
            ),
        );
    }

    public function test_uuid_objects_with_different_values_are_not_equal(): void
    {
        static::assertSame(
            [false],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']], schema(str_schema('id'))),
                array_to_rows([['id' => '00000000-0000-4000-8000-000000000000']], schema(str_schema('id'))),
            ),
        );
    }

    public function test_uuid_objects_with_same_value_are_equal(): void
    {
        static::assertSame(
            [true],
            (new Equal('id', 'id'))->compare(
                array_to_rows([['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']], schema(str_schema('id'))),
                array_to_rows([['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']], schema(str_schema('id'))),
            ),
        );
    }

    public function test_success(): void
    {
        static::assertSame(
            [true],
            (new Equal('datetime', 'datetime'))->compare(
                array_to_rows([[
                    'datetime' => $datetime = new DateTimeImmutable('2022-10-01 00:00:00'),
                ]], schema(datetime_schema('datetime'))),
                array_to_rows([['datetime' => $datetime]], schema(datetime_schema('datetime'))),
            ),
        );
    }

    public function test_compares_pair_aligned_batches(): void
    {
        static::assertSame(
            [true, false, false, true],
            (new Equal('a', 'b'))->compare(
                array_to_rows([
                    ['a' => 1],
                    ['a' => 2],
                    ['a' => null],
                    ['a' => 4],
                ], schema(int_schema('a', nullable: true))),
                array_to_rows([['b' => 1.0], ['b' => 3.0], ['b' => 1.0], ['b' => 4.0]], schema(float_schema('b'))),
            ),
        );
    }
}

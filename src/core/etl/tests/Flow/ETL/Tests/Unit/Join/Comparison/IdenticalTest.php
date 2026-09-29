<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use DateTimeImmutable;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\uuid_schema;

final class IdenticalTest extends FlowTestCase
{
    public function test_failure(): void
    {
        static::assertSame(
            [false],
            (new Identical('id', 'id'))->compare(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => 2]], schema(int_schema('id'))),
            ),
        );
    }

    public function test_null_is_not_identical_to_null(): void
    {
        static::assertSame(
            [false],
            (new Identical('id', 'id'))->compare(
                array_to_rows([['id' => null]], schema(str_schema('id', nullable: true))),
                array_to_rows([['id' => null]], schema(str_schema('id', nullable: true))),
            ),
        );
    }

    public function test_null_is_not_identical_to_value(): void
    {
        static::assertSame(
            [false],
            (new Identical('id', 'id'))->compare(
                array_to_rows([['id' => null]], schema(int_schema('id', nullable: true))),
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
            ),
        );
        static::assertSame(
            [false],
            (new Identical('id', 'id'))->compare(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => null]], schema(int_schema('id', nullable: true))),
            ),
        );
    }

    public function test_success(): void
    {
        static::assertSame(
            [true],
            (new Identical('id', 'id'))->compare(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
            ),
        );
    }

    public function test_compares_pair_aligned_batches(): void
    {
        static::assertSame(
            [true, false, false],
            (new Identical('id', 'id'))->compare(
                array_to_rows([['id' => 1], ['id' => 2], ['id' => null]], schema(int_schema('id', nullable: true))),
                array_to_rows([['id' => 1], ['id' => 3], ['id' => null]], schema(int_schema('id', nullable: true))),
            ),
        );
    }

    public function test_equal_datetimes_are_identical(): void
    {
        static::assertSame(
            [true],
            (new Identical('at', 'at'))->compare(
                array_to_rows([['at' => new DateTimeImmutable('2024-01-01 00:00:00')]], schema(datetime_schema('at'))),
                array_to_rows([['at' => new DateTimeImmutable('2024-01-01 00:00:00')]], schema(datetime_schema('at'))),
            ),
        );
    }

    public function test_equal_uuids_are_identical(): void
    {
        static::assertSame(
            [true, false],
            (new Identical('id', 'id'))->compare(
                array_to_rows([
                    ['id' => '00000000-0000-4000-8000-000000000001'],
                    ['id' => '00000000-0000-4000-8000-000000000001'],
                ], schema(uuid_schema('id'))),
                array_to_rows([
                    ['id' => '00000000-0000-4000-8000-000000000001'],
                    ['id' => '00000000-0000-4000-8000-000000000002'],
                ], schema(uuid_schema('id'))),
            ),
        );
    }

    public function test_values_of_different_types_are_not_identical(): void
    {
        static::assertSame(
            [false],
            (new Identical('id', 'id'))->compare(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => '1']], schema(str_schema('id'))),
            ),
        );
    }
}

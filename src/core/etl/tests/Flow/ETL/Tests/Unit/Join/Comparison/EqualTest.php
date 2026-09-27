<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use DateTimeImmutable;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class EqualTest extends FlowTestCase
{
    public function test_failure(): void
    {
        static::assertFalse((new Equal('datetime', 'datetime'))->compare(
            array_to_row([
                'datetime' => new DateTimeImmutable('2022-10-01 00:00:00'),
            ], schema(datetime_schema('datetime'))),
            array_to_row([
                'datetime' => new DateTimeImmutable('2022-10-01 01:00:00'),
            ], schema(datetime_schema('datetime'))),
        ));
    }

    public function test_null_is_not_equal_to_null(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(
            array_to_row(['id' => null], schema(str_schema('id', nullable: true))),
            array_to_row(['id' => null], schema(str_schema('id', nullable: true))),
        ));
    }

    public function test_null_is_not_equal_to_value(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(
            array_to_row(['id' => null], schema(int_schema('id', nullable: true))),
            array_to_row(['id' => 1], schema(int_schema('id'))),
        ));
        static::assertFalse((new Equal('id', 'id'))->compare(
            array_to_row(['id' => 1], schema(int_schema('id'))),
            array_to_row(['id' => null], schema(int_schema('id', nullable: true))),
        ));
    }

    public function test_object_and_scalar_are_not_equal(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(
            array_to_row(['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479'], schema(str_schema('id'))),
            array_to_row(['id' => 1], schema(int_schema('id'))),
        ));
    }

    public function test_uuid_objects_with_different_values_are_not_equal(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(
            array_to_row(['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479'], schema(str_schema('id'))),
            array_to_row(['id' => '00000000-0000-4000-8000-000000000000'], schema(str_schema('id'))),
        ));
    }

    public function test_uuid_objects_with_same_value_are_equal(): void
    {
        static::assertTrue((new Equal('id', 'id'))->compare(
            array_to_row(['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479'], schema(str_schema('id'))),
            array_to_row(['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479'], schema(str_schema('id'))),
        ));
    }

    public function test_success(): void
    {
        static::assertTrue((new Equal('datetime', 'datetime'))->compare(
            array_to_row([
                'datetime' => $datetime = new DateTimeImmutable('2022-10-01 00:00:00'),
            ], schema(datetime_schema('datetime'))),
            array_to_row(['datetime' => $datetime], schema(datetime_schema('datetime'))),
        ));
    }
}

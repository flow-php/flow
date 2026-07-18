<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use DateTimeImmutable;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\uuid_entry;

final class EqualTest extends FlowTestCase
{
    public function test_failure(): void
    {
        static::assertFalse((new Equal('datetime', 'datetime'))->compare(
            row(datetime_entry('datetime', new DateTimeImmutable('2022-10-01 00:00:00'))),
            row(datetime_entry('datetime', new DateTimeImmutable('2022-10-01 01:00:00'))),
        ));
    }

    public function test_object_and_scalar_are_not_equal(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(
            row(uuid_entry('id', 'f47ac10b-58cc-4372-a567-0e02b2c3d479')),
            row(int_entry('id', 1)),
        ));
    }

    public function test_uuid_objects_with_different_values_are_not_equal(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(
            row(uuid_entry('id', 'f47ac10b-58cc-4372-a567-0e02b2c3d479')),
            row(uuid_entry('id', '00000000-0000-4000-8000-000000000000')),
        ));
    }

    public function test_uuid_objects_with_same_value_are_equal(): void
    {
        static::assertTrue((new Equal('id', 'id'))->compare(
            row(uuid_entry('id', 'f47ac10b-58cc-4372-a567-0e02b2c3d479')),
            row(uuid_entry('id', 'f47ac10b-58cc-4372-a567-0e02b2c3d479')),
        ));
    }

    public function test_success(): void
    {
        static::assertTrue((new Equal('datetime', 'datetime'))->compare(
            row(datetime_entry('datetime', $datetime = new DateTimeImmutable('2022-10-01 00:00:00'))),
            row(datetime_entry('datetime', $datetime)),
        ));
    }
}

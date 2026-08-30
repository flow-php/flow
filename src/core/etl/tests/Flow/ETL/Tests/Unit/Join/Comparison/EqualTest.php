<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use DateTimeImmutable;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\row;

final class EqualTest extends FlowTestCase
{
    public function test_failure(): void
    {
        static::assertFalse((new Equal('datetime', 'datetime'))->compare(
            row(['datetime' => new DateTimeImmutable('2022-10-01 00:00:00')]),
            row(['datetime' => new DateTimeImmutable('2022-10-01 01:00:00')]),
        ));
    }

    public function test_null_is_not_equal_to_null(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(row(['id' => null]), row(['id' => null])));
    }

    public function test_null_is_not_equal_to_value(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(row(['id' => null]), row(['id' => 1])));
        static::assertFalse((new Equal('id', 'id'))->compare(row(['id' => 1]), row(['id' => null])));
    }

    public function test_object_and_scalar_are_not_equal(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(
            row(['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']),
            row(['id' => 1]),
        ));
    }

    public function test_uuid_objects_with_different_values_are_not_equal(): void
    {
        static::assertFalse((new Equal('id', 'id'))->compare(
            row(['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']),
            row(['id' => '00000000-0000-4000-8000-000000000000']),
        ));
    }

    public function test_uuid_objects_with_same_value_are_equal(): void
    {
        static::assertTrue((new Equal('id', 'id'))->compare(
            row(['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']),
            row(['id' => 'f47ac10b-58cc-4372-a567-0e02b2c3d479']),
        ));
    }

    public function test_success(): void
    {
        static::assertTrue((new Equal('datetime', 'datetime'))->compare(
            row(['datetime' => $datetime = new DateTimeImmutable('2022-10-01 00:00:00')]),
            row(['datetime' => $datetime]),
        ));
    }
}

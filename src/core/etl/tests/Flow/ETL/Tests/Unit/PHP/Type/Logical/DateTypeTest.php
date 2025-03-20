<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_date, type_datetime, type_int, type_null};
use Flow\ETL\Tests\FlowTestCase;

final class DateTypeTest extends FlowTestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_date()->isEqual(type_date())
        );
        self::assertFalse(
            type_date()->isEqual(type_int())
        );
    }

    public function test_is_comparable() : void
    {
        self::assertTrue(type_date()->isComparableWith(type_datetime()));
        self::assertTrue(type_date()->isComparableWith(type_date()));
        self::assertTrue(type_date()->isComparableWith(type_null()));
        self::assertFalse(type_date()->isComparableWith(type_int()));
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_date(true)->isValid(null));
        self::assertFalse(type_date()->isValid(new \DateTimeImmutable()));
        self::assertTrue(type_date()->isValid(new \DateTime('2024-12-01')));
        self::assertFalse(type_date()->isValid('2020-01-01'));
        self::assertFalse(type_date()->isValid('2020-01-01 00:00:00'));
    }

    public function test_merge_non_nullable_with_non_nullable() : void
    {
        self::assertFalse(type_date()->merge(type_date())->nullable());
    }

    public function test_merge_non_nullable_with_nullable() : void
    {
        self::assertTrue(type_date()->merge(type_date(true))->nullable());
        self::assertTrue(type_date(true)->merge(type_date(false))->nullable());
    }

    public function test_merge_nullable_with_nullable() : void
    {
        self::assertTrue(type_date(true)->merge(type_date(true))->nullable());
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'date',
            type_date()->toString()
        );
        self::assertSame(
            '?date',
            type_date(true)->toString()
        );
    }
}

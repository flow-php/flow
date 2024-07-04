<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\{type_datetime, type_int};
use PHPUnit\Framework\TestCase;

final class DateTimeTypeTest extends TestCase
{
    public function test_equals() : void
    {
        self::assertTrue(
            type_datetime()->isEqual(type_datetime())
        );
        self::assertFalse(
            type_datetime()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_datetime(true)->isValid(null));
        self::assertTrue(type_datetime()->isValid(new \DateTimeImmutable()));
        self::assertTrue(type_datetime()->isValid(new \DateTime()));
        self::assertFalse(type_datetime()->isValid('2020-01-01'));
        self::assertFalse(type_datetime()->isValid('2020-01-01 00:00:00'));
    }

    public function test_merge_non_nullable_with_non_nullable() : void
    {
        self::assertFalse(type_datetime()->merge(type_datetime())->nullable());
    }

    public function test_merge_non_nullable_with_nullable() : void
    {
        self::assertTrue(type_datetime()->merge(type_datetime(true))->nullable());
        self::assertTrue(type_datetime(true)->merge(type_datetime(false))->nullable());
    }

    public function test_merge_nullable_with_nullable() : void
    {
        self::assertTrue(type_datetime(true)->merge(type_datetime(true))->nullable());
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'datetime',
            type_datetime()->toString()
        );
        self::assertSame(
            '?datetime',
            type_datetime(true)->toString()
        );
    }
}

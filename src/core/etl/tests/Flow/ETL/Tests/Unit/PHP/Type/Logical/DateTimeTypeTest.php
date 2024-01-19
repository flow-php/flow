<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Logical;

use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_int;
use PHPUnit\Framework\TestCase;

final class DateTimeTypeTest extends TestCase
{
    public function test_equals() : void
    {
        $this->assertTrue(
            type_datetime()->isEqual(type_datetime())
        );
        $this->assertFalse(
            type_datetime()->isEqual(type_int())
        );
    }

    public function test_is_valid() : void
    {
        $this->assertTrue(type_datetime()->isValid(new \DateTimeImmutable()));
        $this->assertTrue(type_datetime()->isValid(new \DateTime()));
        $this->assertFalse(type_datetime()->isValid('2020-01-01'));
        $this->assertFalse(type_datetime()->isValid('2020-01-01 00:00:00'));
    }

    public function test_to_string() : void
    {
        $this->assertSame(
            'datetime',
            type_datetime()->toString()
        );
        $this->assertSame(
            '?datetime',
            type_datetime(true)->toString()
        );
    }
}

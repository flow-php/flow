<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\uuid_v4;
use function Flow\ETL\DSL\uuid_v7;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class UuidTest extends TestCase
{
    public function test_uuid4() : void
    {
        $expression = uuid_v4();
        $this->assertTrue(
            Uuid::isValid(
                $expression->eval(Row::create())->toString()
            )
        );
        $this->assertNotSame(
            $expression->eval(Row::create()),
            $expression->eval(Row::create())
        );
    }

    public function test_uuid4_is_unique() : void
    {
        $expression = uuid_v4();

        $this->assertNotEquals(
            $expression->eval(Row::create()),
            $expression->eval(Row::create())
        );
    }

    public function test_uuid7() : void
    {
        $this->assertTrue(
            Uuid::isValid(
                uuid_v7(lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC'))))->eval(Row::create())->toString()
            )
        );
    }

    public function test_uuid7_is_unique() : void
    {
        $dateTime = lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')));
        $this->assertNotEquals(
            uuid_v7($dateTime)->eval(Row::create()),
            uuid_v7($dateTime)->eval(Row::create())
        );
    }

    public function test_uuid7_return_null_for_non_datetime_interface() : void
    {
        $this->assertNull(
            uuid_v7(lit(''))->eval(Row::create())
        );
    }
}

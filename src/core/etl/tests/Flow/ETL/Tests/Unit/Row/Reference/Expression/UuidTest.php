<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rowreference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\uuid_v4;
use function Flow\ETL\DSL\uuid_v7;
use function Flow\ETL\DSL\uuid_v8;
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

    public function test_uuid8() : void
    {
        $this->assertTrue(
            Uuid::isValid(
                uuid_v8(lit("\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff"))->eval(Row::create())->toString()
            )
        );
    }

    public function test_uuid8_generate_same_output_for_same_input() : void
    {
        $bytes = lit("\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff");
        $this->assertEquals(
            uuid_v8($bytes)->eval(Row::create()),
            uuid_v8($bytes)->eval(Row::create())
        );
    }
}

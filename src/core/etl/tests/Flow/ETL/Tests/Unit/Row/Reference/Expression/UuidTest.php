<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Rowreference\Expression;

use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;
use function Flow\ETL\DSL\uuid_v4;
use function Flow\ETL\DSL\uuid_v7;
use function Flow\ETL\DSL\uuid_v8;

final class UuidTest extends TestCase
{
    public function test_uuid4() : void
    {
        $this->assertTrue(
            Uuid::isValid(
                uuid_v4()->eval(Row::create())
            )
        );
    }
    public function test_uuid7() : void
    {
        $this->assertTrue(
            Uuid::isValid(
                uuid_v7(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))->eval(Row::create())
            )
        );
    }
    public function test_uuid8() : void
    {
        $this->assertTrue(
            Uuid::isValid(
                uuid_v8("\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff")->eval(Row::create())
            )
        );
    }
}

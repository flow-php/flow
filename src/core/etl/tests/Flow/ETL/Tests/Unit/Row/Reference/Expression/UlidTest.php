<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ulid;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Uid\Ulid;

final class UlidTest extends TestCase
{
    public function test_ulid() : void
    {
        $expression = ulid();
        $this->assertTrue(
            Ulid::isValid(
                $expression->eval(Row::create())->toBase32()
            )
        );
        $this->assertNotSame(
            $expression->eval(Row::create()),
            $expression->eval(Row::create())
        );
    }

    public function test_ulid_is_unique() : void
    {
        $expression = ulid();

        $this->assertNotEquals(
            $expression->eval(Row::create()),
            $expression->eval(Row::create())
        );
    }

    public function test_ulid_with_invalid_value_returns_null() : void
    {
        $this->assertNull(
            ulid(lit(''))->eval(Row::create())
        );
    }
}

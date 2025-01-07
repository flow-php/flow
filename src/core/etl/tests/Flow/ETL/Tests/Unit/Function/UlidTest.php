<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, ulid};
use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\Uid\Ulid;

final class UlidTest extends FlowTestCase
{
    public function test_ulid() : void
    {
        $expression = ulid();
        self::assertTrue(
            Ulid::isValid(
                $expression->eval(row())->toBase32()
            )
        );
        self::assertNotSame(
            $expression->eval(row()),
            $expression->eval(row())
        );
    }

    public function test_ulid_is_unique() : void
    {
        $expression = ulid();

        self::assertNotEquals(
            $expression->eval(row()),
            $expression->eval(row())
        );
    }

    public function test_ulid_with_invalid_value_returns_null() : void
    {
        self::assertNull(
            ulid(lit(''))->eval(row())
        );
    }
}

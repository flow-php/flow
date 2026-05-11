<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\Uid\Ulid;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\ulid;

final class UlidTest extends FlowTestCase
{
    public function test_ulid(): void
    {
        $expression = ulid();
        $result = $expression->eval(row(), flow_context());
        static::assertInstanceOf(Ulid::class, $result);
        static::assertTrue(Ulid::isValid($result->toBase32()));
        static::assertNotSame($expression->eval(row(), flow_context()), $expression->eval(row(), flow_context()));
    }

    public function test_ulid_is_unique(): void
    {
        $expression = ulid();

        static::assertNotEquals($expression->eval(row(), flow_context()), $expression->eval(row(), flow_context()));
    }

    public function test_ulid_with_invalid_value_returns_null(): void
    {
        static::assertNull(ulid(lit(''))->eval(row(), flow_context()));
    }
}

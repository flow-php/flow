<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\Uid\Ulid;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\ulid;

final class UlidTest extends FlowTestCase
{
    public function test_ulid_produces_a_string_column(): void
    {
        $expression = ulid();
        $result = $expression->eval(row(), flow_context());

        static::assertIsString($result);
        static::assertTrue(Ulid::isValid($result));
        static::assertNotSame($expression->eval(row(), flow_context()), $expression->eval(row(), flow_context()));
    }

    public function test_ulid_is_unique(): void
    {
        $expression = ulid();

        static::assertNotEquals($expression->eval(row(), flow_context()), $expression->eval(row(), flow_context()));
    }

    public function test_ulid_with_invalid_value_returns_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Ulid requires valid ULID string: Invalid ULID.');

        ulid(lit(''))->eval(row(), flow_context());
    }
}

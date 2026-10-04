<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Symfony\Component\Uid\Ulid;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\ulid;

final class UlidTest extends FlowTestCase
{
    public function test_ulid_produces_a_string_column(): void
    {
        $expression = ulid();
        // @mago-ignore analysis:mixed-assignment
        $result = (new FunctionContext(flow_context()))->eval($expression, [], schema());

        static::assertIsString($result);
        static::assertTrue(Ulid::isValid($result));
        static::assertNotSame(
            (new FunctionContext(flow_context()))->eval($expression, [], schema()),
            (new FunctionContext(flow_context()))->eval($expression, [], schema()),
        );
    }

    public function test_ulid_is_unique(): void
    {
        $expression = ulid();

        static::assertNotEquals(
            (new FunctionContext(flow_context()))->eval($expression, [], schema()),
            (new FunctionContext(flow_context()))->eval($expression, [], schema()),
        );
    }

    public function test_ulid_with_invalid_value_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Ulid requires valid ULID string: Invalid ULID');

        (new FunctionContext(flow_context()))->eval(ulid(lit('')), [], schema());
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringWidthTest extends FlowTestCase
{
    public function test_width_ascii_string(): void
    {
        static::assertSame(5, (new FunctionContext(flow_context()))->eval(
            ref('str')->stringWidth(),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_width_empty_string(): void
    {
        static::assertSame(0, (new FunctionContext(flow_context()))->eval(
            ref('str')->stringWidth(),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_width_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringWidth function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->stringWidth(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_width_single_character(): void
    {
        static::assertSame(1, (new FunctionContext(flow_context()))->eval(
            ref('str')->stringWidth(),
            ['str' => 'a'],
            schema(str_schema('str')),
        ));
    }
}

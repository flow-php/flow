<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SlugTest extends FlowTestCase
{
    public function test_a_malformed_separator_operand_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new FunctionContext(flow_context()))->eval(
            ref('value')->slug(lit(123)),
            ['value' => 'hello world'],
            schema(str_schema('value')),
        );
    }

    public function test_ascii_on_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Slug function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->slug(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_slug(): void
    {
        static::assertSame('azcz', (new FunctionContext(flow_context()))->eval(
            ref('str')->slug(),
            ['str' => 'ąźćż'],
            schema(str_schema('str')),
        ));
    }

    public function test_slug_separator(): void
    {
        static::assertSame('Some_Text', (new FunctionContext(flow_context()))->eval(
            ref('str')->slug('_'),
            ['str' => 'Some Text'],
            schema(str_schema('str')),
        ));
    }
}

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

final class StringTitleTest extends FlowTestCase
{
    public function test_string_title(): void
    {
        static::assertSame('Foo ijssel', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringTitle(),
            ['str' => 'foo ijssel'],
            schema(str_schema('str')),
        ));
    }

    public function test_string_title_allwords(): void
    {
        static::assertSame('Foo Ijssel', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringTitle(allWords: true),
            ['str' => 'foo ijssel'],
            schema(str_schema('str')),
        ));
    }

    public function test_string_title_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringTitle function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->stringTitle(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }
}

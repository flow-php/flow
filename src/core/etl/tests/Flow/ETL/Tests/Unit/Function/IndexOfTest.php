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

final class IndexOfTest extends FlowTestCase
{
    public function test_index_of(): void
    {
        static::assertSame(5, (new FunctionContext(flow_context()))->eval(
            ref('str')->indexOf('x', offset: 5),
            ['str' => 'AbBAsxa'],
            schema(str_schema('str')),
        ));

        static::assertSame(0, (new FunctionContext(flow_context()))->eval(
            ref('str')->indexOf('A', ignoreCase: true),
            ['str' => 'abbbbb'],
            schema(str_schema('str')),
        ));

        static::assertSame(5, (new FunctionContext(flow_context()))->eval(
            ref('str')->indexOf('x', offset: 5),
            ['str' => 'AbBAsxa'],
            schema(str_schema('str')),
        ));

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->indexOf('x', offset: 2),
            ['str' => 'Abba'],
            schema(str_schema('str')),
        ));
    }

    public function test_index_of_throws_on_null_needle(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOf function requires non-null string and needle');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->indexOf(ref('needle')),
            ['str' => 'x', 'needle' => null],
            schema(str_schema('str'), str_schema('needle', nullable: true)),
        );
    }

    public function test_index_of_throws_on_null_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IndexOf function requires non-null string and needle');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->indexOf('x'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }
}

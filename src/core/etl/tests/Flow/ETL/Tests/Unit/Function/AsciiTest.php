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

final class AsciiTest extends FlowTestCase
{
    public function test_ascii(): void
    {
        static::assertSame('azcz', (new FunctionContext(flow_context()))->eval(
            ref('str')->ascii(),
            ['str' => 'ąźćż'],
            schema(str_schema('str')),
        ));
    }

    public function test_ascii_on_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Ascii function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->ascii(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }
}

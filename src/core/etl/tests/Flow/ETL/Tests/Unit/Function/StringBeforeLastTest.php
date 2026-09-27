<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringBeforeLastTest extends FlowTestCase
{
    public function test_string_before_last(): void
    {
        static::assertSame('hello w', ref('str')
            ->stringBeforeLast(ref('needle'))
            ->eval(
                array_to_row(
                    ['str' => 'hello world', 'needle' => 'o'],
                    schema(str_schema('str'), str_schema('needle')),
                ),
                flow_context(),
            ));
    }

    public function test_string_before_last_including_needle(): void
    {
        static::assertSame('hello wo', ref('str')
            ->stringBeforeLast(ref('needle'), includeNeedle: true)
            ->eval(
                array_to_row(
                    ['str' => 'hello world', 'needle' => 'o'],
                    schema(str_schema('str'), str_schema('needle')),
                ),
                flow_context(),
            ));
    }

    public function test_string_before_last_returns_empty_string(): void
    {
        static::assertSame('', ref('str')
            ->stringBeforeLast(ref('needle'))
            ->eval(
                array_to_row(['str' => '', 'needle' => 'o'], schema(str_schema('str'), str_schema('needle'))),
                flow_context(),
            ));
    }

    public function test_string_before_last_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringBeforeLast function requires non-null value');

        ref('str')
            ->stringBeforeLast(ref('needle'))
            ->eval(
                array_to_row(
                    ['str' => null, 'needle' => 'o'],
                    schema(str_schema('str', nullable: true), str_schema('needle')),
                ),
                flow_context(),
            );
    }
}

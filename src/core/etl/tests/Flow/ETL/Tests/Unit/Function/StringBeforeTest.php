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

final class StringBeforeTest extends FlowTestCase
{
    public function test_string_before(): void
    {
        static::assertSame('hello ', ref('str')
            ->stringBefore(ref('needle'))
            ->eval(
                array_to_row(
                    ['str' => 'hello world', 'needle' => 'world'],
                    schema(str_schema('str'), str_schema('needle')),
                ),
                flow_context(),
            ));

        static::assertSame('hell', ref('str')
            ->stringBefore(ref('needle'))
            ->eval(
                array_to_row(
                    ['str' => 'hello world', 'needle' => 'o'],
                    schema(str_schema('str'), str_schema('needle')),
                ),
                flow_context(),
            ));
    }

    public function test_string_before_including_needle(): void
    {
        static::assertSame('hello', ref('str')
            ->stringBefore(ref('needle'), includeNeedle: true)
            ->eval(
                array_to_row(
                    ['str' => 'hello world', 'needle' => 'o'],
                    schema(str_schema('str'), str_schema('needle')),
                ),
                flow_context(),
            ));
    }

    public function test_string_before_returns_empty_string(): void
    {
        static::assertSame('', ref('str')
            ->stringBefore(ref('needle'))
            ->eval(
                array_to_row(['str' => '', 'needle' => 'o'], schema(str_schema('str'), str_schema('needle'))),
                flow_context(),
            ));
    }

    public function test_string_before_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringBefore function requires non-null value');

        ref('str')
            ->stringBefore(ref('needle'))
            ->eval(
                array_to_row(
                    ['str' => null, 'needle' => 'o'],
                    schema(str_schema('str', nullable: true), str_schema('needle')),
                ),
                flow_context(),
            );
    }
}

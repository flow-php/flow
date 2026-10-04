<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\NumberFormat;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\number_format;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class NumberFormatTest extends FlowTestCase
{
    public function test_number_format(): void
    {
        $expression = new NumberFormat(
            ref('value'),
            ref('decimals'),
            ref('decimal_separator'),
            ref('thousands_separator'),
        );

        static::assertSame('1,234.57', (new FunctionContext(flow_context()))->eval(
            $expression,
            [
                'value' => 1234.5678,
                'decimals' => 2,
                'decimal_separator' => '.',
                'thousands_separator' => ',',
            ],
            schema(
                float_schema('value'),
                int_schema('decimals'),
                str_schema('decimal_separator'),
                str_schema('thousands_separator'),
            ),
        ));
    }

    public function test_number_format_dsl(): void
    {
        $expression = number_format(ref('value'), lit(2), lit('.'), lit(','));

        static::assertSame('1,234.57', (new FunctionContext(flow_context()))->eval(
            $expression,
            [
                'value' => 1234.5678,
            ],
            schema(float_schema('value')),
        ));
    }

    public function test_number_format_on_decimals_that_are_not_integer(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "integer", got "float".');

        $expression = new NumberFormat(
            ref('value'),
            ref('decimals'),
            ref('decimal_separator'),
            ref('thousands_separator'),
        );

        (new FunctionContext(flow_context()))->eval(
            $expression,
            [
                'value' => 1234.5678,
                'decimals' => 2.5,
                'decimal_separator' => '.',
                'thousands_separator' => ',',
            ],
            schema(
                float_schema('value'),
                float_schema('decimals'),
                str_schema('decimal_separator'),
                str_schema('thousands_separator'),
            ),
        );
    }

    public function test_number_format_on_non_int_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "numeric", got "string".');

        $expression = new NumberFormat(
            ref('value'),
            ref('decimals'),
            ref('decimal_separator'),
            ref('thousands_separator'),
        );

        (new FunctionContext(flow_context()))->eval(
            $expression,
            [
                'value' => 'test',
                'decimals' => 2,
                'decimal_separator' => '.',
                'thousands_separator' => ',',
            ],
            schema(
                str_schema('value'),
                int_schema('decimals'),
                str_schema('decimal_separator'),
                str_schema('thousands_separator'),
            ),
        );
    }
}

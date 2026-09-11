<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\NumberFormat;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\number_format;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

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

        static::assertSame('1,234.57', $expression->eval(row([
            'value' => 1234.5678,
            'decimals' => 2,
            'decimal_separator' => '.',
            'thousands_separator' => ',',
        ]), flow_context()));
    }

    public function test_number_format_dsl(): void
    {
        $expression = number_format(ref('value'), lit(2), lit('.'), lit(','));

        static::assertSame('1,234.57', $expression->eval(row(['value' => 1234.5678]), flow_context()));
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

        $expression->eval(row([
            'value' => 1234.5678,
            'decimals' => 2.5,
            'decimal_separator' => '.',
            'thousands_separator' => ',',
        ]), flow_context());
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

        $expression->eval(row([
            'value' => 'test',
            'decimals' => 2,
            'decimal_separator' => '.',
            'thousands_separator' => ',',
        ]), flow_context());
    }
}

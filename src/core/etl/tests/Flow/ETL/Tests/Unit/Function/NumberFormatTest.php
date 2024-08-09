<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{float_entry, int_entry, lit, ref, str_entry};
use Flow\ETL\Function\NumberFormat;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class NumberFormatTest extends TestCase
{
    public function test_number_format() : void
    {
        $expression = new NumberFormat(
            ref('value'),
            ref('decimals'),
            ref('decimal_separator'),
            ref('thousands_separator')
        );

        self::assertSame(
            '1,234.57',
            $expression->eval(
                Row::create(
                    float_entry('value', 1234.5678),
                    int_entry('decimals', 2),
                    str_entry('decimal_separator', '.'),
                    str_entry('thousands_separator', ',')
                )
            )
        );
    }

    public function test_number_format_dsl() : void
    {
        $expression = \Flow\ETL\DSL\number_format(
            ref('value'),
            lit(2),
            lit('.'),
            lit(',')
        );

        self::assertSame(
            '1,234.57',
            $expression->eval(
                Row::create(
                    float_entry('value', 1234.5678),
                )
            )
        );
    }

    public function test_number_format_on_decimals_that_are_not_integer() : void
    {
        $expression = new NumberFormat(
            ref('value'),
            ref('decimals'),
            ref('decimal_separator'),
            ref('thousands_separator')
        );

        self::assertNull(
            $expression->eval(
                Row::create(
                    float_entry('value', 1234.5678),
                    float_entry('decimals', 2.5),
                    str_entry('decimal_separator', '.'),
                    str_entry('thousands_separator', ',')
                )
            )
        );
    }

    public function test_number_format_on_non_int_entry() : void
    {
        $expression = new NumberFormat(
            ref('value'),
            ref('decimals'),
            ref('decimal_separator'),
            ref('thousands_separator')
        );

        self::assertNull(
            $expression->eval(
                Row::create(
                    str_entry('value', 'test'),
                    int_entry('decimals', 2),
                    str_entry('decimal_separator', '.'),
                    str_entry('thousands_separator', ',')
                )
            )
        );
    }
}

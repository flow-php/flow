<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer\ScalarFunctionTransformer;

use function Flow\ETL\DSL\{float_entry, flow_context, int_entry, integer_entry, ref, row, rows};
use Flow\Calculator\Rounding;
use Flow\ETL\Row\Entry\{FloatEntry, IntegerEntry};
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use PHPUnit\Framework\Attributes\{DataProvider};

final class MathTest extends FlowTestCase
{
    public static function divide_data_provider() : \Generator
    {
        yield [
            float_entry('a', 0.3),
            float_entry('b', -0.1),
            null,
            null,
            ['result' => -3, 'a' => 0.3, 'b' => -0.1],
        ];

        yield [
            float_entry('a', 0.0003),
            float_entry('b', 0.00000017),
            6,
            Rounding::HALF_UP,
            ['result' => 1764.705882, 'a' => 0.0003, 'b' => 0.00000017],
        ];
    }

    public static function minus_data_provider() : \Generator
    {
        yield [
            float_entry('a', 0.3),
            float_entry('b', 0.1),
            ['result' => 0.2, 'a' => 0.3, 'b' => 0.1],
        ];

        yield [
            float_entry('a', 0.0000003),
            float_entry('b', 0.0000001),
            ['result' => 0.0000002, 'a' => 0.0000003, 'b' => 0.0000001],
        ];
        yield [
            float_entry('a', 0.3),
            float_entry('b', 0.1),
            ['result' => 0.2, 'a' => 0.3_0000_0000_0000_000, 'b' => 0.1_0000_0000_0000_000],
        ];
    }

    public static function multiply_data_provider() : \Generator
    {
        yield [
            float_entry('a', 0.3),
            float_entry('b', -0.1),
            ['result' => -0.03, 'a' => 0.3, 'b' => -0.1],
        ];

        yield [
            float_entry('a', 0.0000003),
            float_entry('b', -0.0000001),
            ['result' => -0.00000000000003, 'a' => 0.0000003, 'b' => -0.0000001],
        ];
    }

    public static function plus_data_provider() : \Generator
    {
        yield [
            float_entry('a', 0.3),
            float_entry('b', -0.1),
            ['result' => 0.2, 'a' => 0.3, 'b' => -0.1],
        ];

        yield [
            float_entry('a', 0.0000003),
            float_entry('b', -0.0000001),
            ['result' => 0.0000002, 'a' => 0.0000003, 'b' => -0.0000001],
        ];
    }

    public static function power_data_provider() : \Generator
    {
        yield [
            float_entry('a', -0.3),
            int_entry('b', 1),
            ['result' => -0.3, 'a' => -0.3, 'b' => 1],
        ];

        yield [
            float_entry('a', -0.3),
            integer_entry('b', 10),
            ['result' => 5.9049E-6, 'a' => -0.3, 'b' => 10],
        ];
    }

    /**
     * @param array<string, mixed> $result
     */
    #[DataProvider('divide_data_provider')]
    public function test_divide(IntegerEntry|FloatEntry $a, IntegerEntry|FloatEntry $b, ?int $scale, ?Rounding $rounding, array $result) : void
    {
        $rows = (new ScalarFunctionTransformer('result', ref($a->name())->divide(ref($b->name()), $scale, $rounding)))
            ->transform(
                rows(row($a, $b)),
                flow_context()
            );

        self::assertEquals(
            [
                $result,
            ],
            $rows->toArray()
        );
    }

    /**
     * @param array<string, mixed> $result
     */
    #[DataProvider('minus_data_provider')]
    public function test_minus(IntegerEntry|FloatEntry $a, IntegerEntry|FloatEntry $b, array $result) : void
    {
        $rows = (new ScalarFunctionTransformer('result', ref($a->name())->minus(ref($b->name()))))
            ->transform(
                rows(row($a, $b)),
                flow_context()
            );

        self::assertEquals(
            [
                $result,
            ],
            $rows->toArray()
        );
    }

    /**
     * @param array<string, mixed> $result
     */
    #[DataProvider('multiply_data_provider')]
    public function test_multiply(IntegerEntry|FloatEntry $a, IntegerEntry|FloatEntry $b, array $result) : void
    {
        $rows = (new ScalarFunctionTransformer('result', ref($a->name())->multiply(ref($b->name()))))
            ->transform(
                rows(row($a, $b)),
                flow_context()
            );

        self::assertEquals(
            [
                $result,
            ],
            $rows->toArray()
        );
    }

    /**
     * @param array<string, mixed> $result
     */
    #[DataProvider('plus_data_provider')]
    public function test_plus(IntegerEntry|FloatEntry $a, IntegerEntry|FloatEntry $b, array $result) : void
    {
        $rows = (new ScalarFunctionTransformer('result', ref($a->name())->plus(ref($b->name()))))
            ->transform(
                rows(row($a, $b)),
                flow_context()
            );

        self::assertEquals(
            [
                $result,
            ],
            $rows->toArray()
        );
    }

    /**
     * @param array<string, mixed> $result
     */
    #[DataProvider('power_data_provider')]
    public function test_power(IntegerEntry|FloatEntry $a, IntegerEntry|FloatEntry $b, array $result) : void
    {
        $rows = (new ScalarFunctionTransformer('result', ref($a->name())->power(ref($b->name()))))
            ->transform(
                rows(row($a, $b)),
                flow_context()
            );

        self::assertEquals(
            [
                $result,
            ],
            $rows->toArray()
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer\ScalarFunctionTransformer;

use Flow\Calculator\Rounding;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class MathTest extends FlowTestCase
{
    public static function divide_data_provider(): Generator
    {
        yield [
            float_schema('a'),
            0.3,
            float_schema('b'),
            -0.1,
            null,
            null,
            ['result' => -3, 'a' => 0.3, 'b' => -0.1],
        ];

        yield [
            float_schema('a'),
            0.0003,
            float_schema('b'),
            0.00000017,
            6,
            Rounding::HALF_UP,
            ['result' => 1764.705882, 'a' => 0.0003, 'b' => 0.00000017],
        ];
    }

    public static function minus_data_provider(): Generator
    {
        yield [
            float_schema('a'),
            0.3,
            float_schema('b'),
            0.1,
            ['result' => 0.2, 'a' => 0.3, 'b' => 0.1],
        ];

        yield [
            float_schema('a'),
            0.0000003,
            float_schema('b'),
            0.0000001,
            ['result' => 0.0000002, 'a' => 0.0000003, 'b' => 0.0000001],
        ];

        yield [
            float_schema('a'),
            0.3,
            float_schema('b'),
            0.1,
            ['result' => 0.2, 'a' => 0.3_0000_0000_0000_000, 'b' => 0.1_0000_0000_0000_000],
        ];
    }

    public static function multiply_data_provider(): Generator
    {
        yield [
            float_schema('a'),
            0.3,
            float_schema('b'),
            -0.1,
            ['result' => -0.03, 'a' => 0.3, 'b' => -0.1],
        ];

        yield [
            float_schema('a'),
            0.0000003,
            float_schema('b'),
            -0.0000001,
            ['result' => -0.00000000000003, 'a' => 0.0000003, 'b' => -0.0000001],
        ];
    }

    public static function plus_data_provider(): Generator
    {
        yield [
            float_schema('a'),
            0.3,
            float_schema('b'),
            -0.1,
            ['result' => 0.2, 'a' => 0.3, 'b' => -0.1],
        ];

        yield [
            float_schema('a'),
            0.0000003,
            float_schema('b'),
            -0.0000001,
            ['result' => 0.0000002, 'a' => 0.0000003, 'b' => -0.0000001],
        ];
    }

    public static function power_data_provider(): Generator
    {
        yield [
            float_schema('a'),
            -0.3,
            int_schema('b'),
            1,
            ['result' => -0.3, 'a' => -0.3, 'b' => 1],
        ];

        yield [
            float_schema('a'),
            -0.3,
            integer_schema('b'),
            10,
            ['result' => 5.9049E-6, 'a' => -0.3, 'b' => 10],
        ];
    }

    /**
     * @param Definition<mixed> $a
     * @param Definition<mixed> $b
     * @param array<string, mixed> $result
     */
    #[DataProvider('divide_data_provider')]
    public function test_divide(
        Definition $a,
        mixed $aValue,
        Definition $b,
        mixed $bValue,
        ?int $scale,
        ?Rounding $rounding,
        array $result,
    ): void {
        $rows = (new ScalarFunctionTransformer('result', ref($a->entry()->name())
            ->divide(ref($b->entry()->name()), $scale, $rounding)))->transform(
            rows(schema($a, $b), row([$a->entry()->name() => $aValue, $b->entry()->name() => $bValue])),
            flow_context(),
        );

        static::assertEquals([$result], $rows->toArray());
    }

    /**
     * @param Definition<mixed> $a
     * @param Definition<mixed> $b
     * @param array<string, mixed> $result
     */
    #[DataProvider('minus_data_provider')]
    public function test_minus(Definition $a, mixed $aValue, Definition $b, mixed $bValue, array $result): void
    {
        $rows = (new ScalarFunctionTransformer(
            'result',
            ref($a->entry()->name())->minus(ref($b->entry()->name())),
        ))->transform(
            rows(schema($a, $b), row([$a->entry()->name() => $aValue, $b->entry()->name() => $bValue])),
            flow_context(),
        );

        static::assertEquals([$result], $rows->toArray());
    }

    /**
     * @param Definition<mixed> $a
     * @param Definition<mixed> $b
     * @param array<string, mixed> $result
     */
    #[DataProvider('multiply_data_provider')]
    public function test_multiply(Definition $a, mixed $aValue, Definition $b, mixed $bValue, array $result): void
    {
        $rows = (new ScalarFunctionTransformer(
            'result',
            ref($a->entry()->name())->multiply(ref($b->entry()->name())),
        ))->transform(
            rows(schema($a, $b), row([$a->entry()->name() => $aValue, $b->entry()->name() => $bValue])),
            flow_context(),
        );

        static::assertEquals([$result], $rows->toArray());
    }

    /**
     * @param Definition<mixed> $a
     * @param Definition<mixed> $b
     * @param array<string, mixed> $result
     */
    #[DataProvider('plus_data_provider')]
    public function test_plus(Definition $a, mixed $aValue, Definition $b, mixed $bValue, array $result): void
    {
        $rows = (new ScalarFunctionTransformer(
            'result',
            ref($a->entry()->name())->plus(ref($b->entry()->name())),
        ))->transform(
            rows(schema($a, $b), row([$a->entry()->name() => $aValue, $b->entry()->name() => $bValue])),
            flow_context(),
        );

        static::assertEquals([$result], $rows->toArray());
    }

    /**
     * @param Definition<mixed> $a
     * @param Definition<mixed> $b
     * @param array<string, mixed> $result
     */
    #[DataProvider('power_data_provider')]
    public function test_power(Definition $a, mixed $aValue, Definition $b, mixed $bValue, array $result): void
    {
        $rows = (new ScalarFunctionTransformer(
            'result',
            ref($a->entry()->name())->power(ref($b->entry()->name())),
        ))->transform(
            rows(schema($a, $b), row([$a->entry()->name() => $aValue, $b->entry()->name() => $bValue])),
            flow_context(),
        );

        static::assertEquals([$result], $rows->toArray());
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\sum;

final class MathTest extends FlowTestCase
{
    public function test_aggregations_on_floats(): void
    {
        $rows = rows(schema());
        df()
            ->read(from_rows(rows(
                schema(integer_schema('id'), float_schema('price'), integer_schema('quantity'), float_schema('weight')),
                row(['id' => 1, 'price' => 29.39, 'quantity' => 2, 'weight' => 1.5]),
                row(['id' => 2, 'price' => 19.3, 'quantity' => 1, 'weight' => 0.5]),
                row(['id' => 3, 'price' => 39.1, 'quantity' => 3, 'weight' => 2.0]),
                row(['id' => 4, 'price' => 49.9, 'quantity' => 4, 'weight' => 2.1284]),
                row(['id' => 5, 'price' => 15.0, 'quantity' => 1, 'weight' => 0.3]),
                row(['id' => 6, 'price' => 30.0, 'quantity' => 2, 'weight' => 1.0]),
                row(['id' => 7, 'price' => 25.0, 'quantity' => 3, 'weight' => 1.8]),
                row(['id' => 8, 'price' => 20.0, 'quantity' => 1, 'weight' => 0.8]),
                row(['id' => 9, 'price' => 35.0, 'quantity' => 4, 'weight' => 2.2]),
                row(['id' => 10, 'price' => 45.0, 'quantity' => 5, 'weight' => 3.0]),
            )))
            ->aggregate([sum(ref('price')), sum(ref('weight'))])
            ->run(static function (Rows $r) use (&$rows): void {
                $rows = $rows->merge($r);
            }, analyze: analyze()->withSchema());

        static::assertSame(
            [
                ['price_sum' => 307.69, 'weight_sum' => 15.2284],
            ],
            $rows->toArray(),
        );
    }

    public function test_mathematical_operations_on_floats(): void
    {
        $rows = rows(schema());
        df()
            ->read(from_rows(rows(
                schema(integer_schema('id'), float_schema('price'), integer_schema('quantity'), float_schema('weight')),
                row(['id' => 1, 'price' => 29.39, 'quantity' => 2, 'weight' => 1.5]),
                row(['id' => 2, 'price' => 19.3, 'quantity' => 1, 'weight' => 0.5]),
                row(['id' => 3, 'price' => 39.1, 'quantity' => 3, 'weight' => 2.0]),
                row(['id' => 4, 'price' => 49.9, 'quantity' => 4, 'weight' => 2.1284]),
                row(['id' => 5, 'price' => 15.0, 'quantity' => 1, 'weight' => 0.3]),
                row(['id' => 6, 'price' => 30.0, 'quantity' => 2, 'weight' => 1.0]),
                row(['id' => 7, 'price' => 25.0, 'quantity' => 3, 'weight' => 1.8]),
                row(['id' => 8, 'price' => 20.0, 'quantity' => 1, 'weight' => 0.8]),
                row(['id' => 9, 'price' => 35.0, 'quantity' => 4, 'weight' => 2.2]),
                row(['id' => 10, 'price' => 45.0, 'quantity' => 5, 'weight' => 3.0]),
            )))
            ->withEntry('discount', ref('price')->multiply(-0.1))
            ->withEntry('total_weight', ref('weight')->multiply(ref('quantity')))
            ->run(static function (Rows $r) use (&$rows): void {
                $rows = $rows->merge($r);
            }, analyze: analyze()->withSchema());

        static::assertEquals(
            [
                [
                    'id' => 1,
                    'price' => 29.39,
                    'quantity' => 2,
                    'weight' => 1.5,
                    'discount' => -2.939,
                    'total_weight' => 3.0,
                ],
                [
                    'id' => 2,
                    'price' => 19.3,
                    'quantity' => 1,
                    'weight' => 0.5,
                    'discount' => -1.93,
                    'total_weight' => 0.5,
                ],
                [
                    'id' => 3,
                    'price' => 39.1,
                    'quantity' => 3,
                    'weight' => 2.0,
                    'discount' => -3.91,
                    'total_weight' => 6.0,
                ],
                [
                    'id' => 4,
                    'price' => 49.9,
                    'quantity' => 4,
                    'weight' => 2.1284,
                    'discount' => -4.99,
                    'total_weight' => 8.5136,
                ],
                [
                    'id' => 5,
                    'price' => 15.0,
                    'quantity' => 1,
                    'weight' => 0.3,
                    'discount' => -1.5,
                    'total_weight' => 0.3,
                ],
                [
                    'id' => 6,
                    'price' => 30.0,
                    'quantity' => 2,
                    'weight' => 1.0,
                    'discount' => -3.0,
                    'total_weight' => 2.0,
                ],
                [
                    'id' => 7,
                    'price' => 25.0,
                    'quantity' => 3,
                    'weight' => 1.8,
                    'discount' => -2.5,
                    'total_weight' => 5.4,
                ],
                [
                    'id' => 8,
                    'price' => 20.0,
                    'quantity' => 1,
                    'weight' => 0.8,
                    'discount' => -2.0,
                    'total_weight' => 0.8,
                ],
                [
                    'id' => 9,
                    'price' => 35.0,
                    'quantity' => 4,
                    'weight' => 2.2,
                    'discount' => -3.5,
                    'total_weight' => 8.8,
                ],
                [
                    'id' => 10,
                    'price' => 45.0,
                    'quantity' => 5,
                    'weight' => 3.0,
                    'discount' => -4.5,
                    'total_weight' => 15.0,
                ],
            ],
            $rows->toArray(),
        );
    }
}

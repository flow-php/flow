<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\{config, row, rows};
use function Flow\ETL\DSL\{float_entry, flow_context, integer_entry, string_entry};
use Flow\ETL\Transformer\GroupToArrayTransformer;
use Flow\ETL\{Tests\FlowTestCase};

final class GroupToArrayTransformerTest extends FlowTestCase
{
    public function test_grouping_entries_to_array() : void
    {
        $rows = rows(row(integer_entry('order_id', 1), string_entry('sku', 'SKU-01'), integer_entry('quantity', 1), float_entry('price', 10.00), string_entry('currency', 'PLN')), row(integer_entry('order_id', 1), string_entry('sku', 'SKU-02'), integer_entry('quantity', 1), float_entry('price', 5.00), string_entry('currency', 'PLN')), row(integer_entry('order_id', 2), string_entry('sku', 'SKU-01'), integer_entry('quantity', 1), float_entry('price', 10.00), string_entry('currency', 'PLN')));

        $transformer = new GroupToArrayTransformer('order_id', 'order_line_items');

        self::assertSame(
            [
                [
                    'order_line_items' => [
                        [
                            'order_id' => 1,
                            'sku' => 'SKU-01',
                            'quantity' => 1,
                            'price' => 10.0,
                            'currency' => 'PLN',
                        ],
                        [
                            'order_id' => 1,
                            'sku' => 'SKU-02',
                            'quantity' => 1,
                            'price' => 5.0,
                            'currency' => 'PLN',
                        ],
                    ],
                ],
                [
                    'order_line_items' => [
                        [
                            'order_id' => 2,
                            'sku' => 'SKU-01',
                            'quantity' => 1,
                            'price' => 10.0,
                            'currency' => 'PLN',
                        ],
                    ],
                ],
            ],
            $transformer->transform($rows, flow_context(config()))->toArray()
        );
    }
}

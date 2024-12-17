<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Transformer\GroupToArrayTransformer;
use Flow\ETL\{Config, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class GroupToArrayTransformerTest extends TestCase
{
    public function test_grouping_entries_to_array() : void
    {
        $rows = new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('order_id', 1),
                string_entry('sku', 'SKU-01'),
                new Row\Entry\IntegerEntry('quantity', 1),
                new Row\Entry\FloatEntry('price', 10.00),
                string_entry('currency', 'PLN'),
            ),
            Row::create(
                new Row\Entry\IntegerEntry('order_id', 1),
                string_entry('sku', 'SKU-02'),
                new Row\Entry\IntegerEntry('quantity', 1),
                new Row\Entry\FloatEntry('price', 5.00),
                string_entry('currency', 'PLN'),
            ),
            Row::create(
                new Row\Entry\IntegerEntry('order_id', 2),
                string_entry('sku', 'SKU-01'),
                new Row\Entry\IntegerEntry('quantity', 1),
                new Row\Entry\FloatEntry('price', 10.00),
                string_entry('currency', 'PLN'),
            )
        );

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
            $transformer->transform($rows, new FlowContext(Config::default()))->toArray()
        );
    }
}

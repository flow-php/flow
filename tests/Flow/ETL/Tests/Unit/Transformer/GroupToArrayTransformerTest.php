<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class GroupToArrayTransformerTest extends TestCase
{
    public function test_grouping_entries_to_array() : void
    {
        $rows = new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('order_id', 1),
                new Row\Entry\StringEntry('sku', 'SKU-01'),
                new Row\Entry\IntegerEntry('quantity', 1),
                new Row\Entry\FloatEntry('price', 10.00),
                new Row\Entry\StringEntry('currency', 'PLN'),
            ),
            Row::create(
                new Row\Entry\IntegerEntry('order_id', 1),
                new Row\Entry\StringEntry('sku', 'SKU-02'),
                new Row\Entry\IntegerEntry('quantity', 1),
                new Row\Entry\FloatEntry('price', 5.00),
                new Row\Entry\StringEntry('currency', 'PLN'),
            ),
            Row::create(
                new Row\Entry\IntegerEntry('order_id', 2),
                new Row\Entry\StringEntry('sku', 'SKU-01'),
                new Row\Entry\IntegerEntry('quantity', 1),
                new Row\Entry\FloatEntry('price', 10.00),
                new Row\Entry\StringEntry('currency', 'PLN'),
            )
        );

        $transformer = Transform::group_to_array('order_id', 'order_line_items');

        $this->assertSame(
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
            $transformer->transform($rows)->toArray()
        );
    }
}

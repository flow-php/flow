<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\{datetime_schema, df, int_schema, ref, schema, type_int};
use function Flow\Filesystem\DSL\path;
use Flow\ETL\Tests\FlowIntegrationTestCase;

final class XMLTest extends FlowIntegrationTestCase
{
    public function test_transforming_xml_into_a_tabular_dataset() : void
    {
        $rows = df()
            ->read(from_xml(path(__DIR__ . '/../Fixtures/simple_items.xml'))->withXMLNodePath('root/items'))
            ->withEntry('parent_attribute_01', ref('node')->domElementAttributeValue('items_attribute_01')->cast(type_int()))
            ->withEntry('parent_attribute_02', ref('node')->domElementAttributeValue('items_attribute_02')->cast(type_int()))
            ->withEntry('items', ref('node')->xpath('/*/item'))
            ->withEntry('item', ref('items')->expand())
            ->withEntry('item_attribute_01', ref('item')->domElementAttributeValue('item_attribute_01')->cast(type_int()))
            ->withEntry('value', ref('item')->cast(type_int()))
            ->drop('node', 'items', 'item')
            ->fetch();

        self::assertEquals(
            [
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 1, 'value' => 1],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 2, 'value' => 2],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 3, 'value' => 3],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 4, 'value' => 4],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 5, 'value' => 5],
            ],
            $rows->toArray()
        );

        self::assertEquals(
            schema(
                int_schema('parent_attribute_01'),
                int_schema('parent_attribute_02'),
                int_schema('item_attribute_01'),
                int_schema('value')
            ),
            $rows->schema()
        );
    }

    public function test_transforming_xml_into_a_tabular_dataset_from_partitioned_dataset() : void
    {
        $rows = df()
            ->read(from_xml(path(__DIR__ . '/../Fixtures/partitioned/date=*/*.xml'))->withXMLNodePath('root/items'))

            ->withEntry('parent_attribute_01', ref('node')->domElementAttributeValue('items_attribute_01')->cast(type_int()))
            ->withEntry('parent_attribute_02', ref('node')->domElementAttributeValue('items_attribute_02')->cast(type_int()))
            ->withEntry('items', ref('node')->xpath('/*/item'))
            ->withEntry('item', ref('items')->expand())
            ->withEntry('item_attribute_01', ref('item')->domElementAttributeValue('item_attribute_01')->cast(type_int()))
            ->withEntry('value', ref('item')->cast(type_int()))
            ->withEntry('date', ref('date')->cast('date'))
            ->sortBy(ref('date')->asc())
            ->drop('node', 'items', 'item')
            ->fetch();

        self::assertEquals(
            [
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 1, 'value' => 1, 'date' => new \DateTimeImmutable('2024-08-01')],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 2, 'value' => 2, 'date' => new \DateTimeImmutable('2024-08-01')],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 3, 'value' => 3, 'date' => new \DateTimeImmutable('2024-08-01')],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 4, 'value' => 4, 'date' => new \DateTimeImmutable('2024-08-01')],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 5, 'value' => 5, 'date' => new \DateTimeImmutable('2024-08-01')],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 6, 'value' => 6, 'date' => new \DateTimeImmutable('2024-08-02')],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 7, 'value' => 7, 'date' => new \DateTimeImmutable('2024-08-03')],
                ['parent_attribute_01' => 1, 'parent_attribute_02' => 2, 'item_attribute_01' => 8, 'value' => 8, 'date' => new \DateTimeImmutable('2024-08-03')],
            ],
            $rows->toArray()
        );

        self::assertEquals(
            schema(
                int_schema('parent_attribute_01'),
                int_schema('parent_attribute_02'),
                int_schema('item_attribute_01'),
                int_schema('value'),
                datetime_schema('date')
            ),
            $rows->schema()
        );
    }
}

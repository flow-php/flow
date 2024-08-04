<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Tests\Integration;

use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\{df, int_schema, ref, schema, type_int};
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\Filesystem\Path;

final class XMLTest extends IntegrationTestCase
{
    public function test_reading_xml_and_converting_it_to_rows() : void
    {
        $rows = df()
            ->read(from_xml(Path::realpath(__DIR__ . '/../Fixtures/simple_items.xml'), 'root/items'))
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
}

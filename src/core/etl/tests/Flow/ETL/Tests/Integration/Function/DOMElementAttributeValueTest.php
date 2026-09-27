<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class DOMElementAttributeValueTest extends FlowTestCase
{
    public function test_dom_element_attribute_value(): void
    {
        $rows = df()
            ->read(from_rows(array_to_rows([[
                'node' => type_xml_element()->cast('<name id="1">User Name 01</name>'),
            ]], schema(xml_element_schema('node')))))
            ->withEntry('user_id', ref('node')->domElementAttributeValue('id'))
            ->drop('node')
            ->fetch();

        static::assertSame(
            [
                ['user_id' => '1'],
            ],
            $rows->toArray(),
        );
    }

    public function test_dom_element_attribute_value_from_dom_document(): void
    {
        $rows = df()
            ->read(from_rows(array_to_rows([[
                'node' => type_xml()->cast('<name id="1">User Name 01</name>'),
            ]], schema(xml_schema('node')))))
            ->withEntry('user_id', ref('node')->domElementAttributeValue('id'))
            ->drop('node')
            ->fetch();

        static::assertSame(
            [
                ['user_id' => '1'],
            ],
            $rows->toArray(),
        );
    }

    public function test_dom_element_attribute_value_on_xpath_result(): void
    {
        $rows = df()
            ->read(from_rows(array_to_rows([[
                'node' => type_xml()->cast('<user><name id="1">User Name 01</name></user>'),
            ]], schema(xml_schema('node')))))
            ->withEntry('user_id', ref('node')->xpath('name')->domElementAttributeValue('id'))
            ->drop('node')
            ->fetch();

        static::assertSame(
            [
                ['user_id' => '1'],
            ],
            $rows->toArray(),
        );
    }
}

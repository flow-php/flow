<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use DOMDocument;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class DOMElementValueTest extends FlowTestCase
{
    public function test_dom_element_cast_as_string(): void
    {
        $document = new DOMDocument();
        $document->loadXml('<b>User Name 01</b>');

        static::assertSame(
            [
                ['html' => '<b>User Name 01</b>'],
            ],
            df()
                ->read(from_rows(array_to_rows([['html_raw' => $document]], schema(xml_schema('html_raw')))))
                ->withColumn('html', ref('html_raw')->cast(type_string()))
                ->drop('html_raw')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_dom_element_value(): void
    {
        static::assertSame(
            [
                ['user_name' => 'User Name 01'],
            ],
            df()
                ->read(from_rows(array_to_rows([[
                    'node' => type_xml_element()->cast('<name>User Name 01</name>'),
                ]], schema(xml_element_schema('node')))))
                ->withColumn('user_name', ref('node')->domElementValue())
                ->drop('node')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_dom_element_value_from_dom_document(): void
    {
        static::assertSame(
            [
                ['user_name' => 'User Name 01'],
            ],
            df()
                ->read(from_rows(array_to_rows([[
                    'node' => type_xml()->cast('<name>User Name 01</name>'),
                ]], schema(xml_schema('node')))))
                ->withColumn('user_name', ref('node')->domElementValue())
                ->drop('node')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_dom_element_value_on_dom_document(): void
    {
        $document = new DOMDocument();
        $document->loadXml('<b>User Name 01</b>');

        static::assertSame(
            [
                ['html' => 'User Name 01'],
            ],
            df()
                ->read(from_rows(array_to_rows([['html_raw' => $document]], schema(xml_schema('html_raw')))))
                ->withColumn('html', ref('html_raw')->domElementValue())
                ->drop('html_raw')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_dom_element_value_on_xpath_result(): void
    {
        static::assertSame(
            [
                ['user_name' => 'User Name 01'],
            ],
            df()
                ->read(from_rows(array_to_rows([[
                    'node' => type_xml()->cast('<user><name>User Name 01</name></user>'),
                ]], schema(xml_schema('node')))))
                ->withColumn('user_name', ref('node')->xpath('name')->domElementValue())
                ->drop('node')
                ->fetch()
                ->toArray(),
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\xml_element_entry;
use function Flow\ETL\DSL\xml_entry;

final class DOMElementAttributeValueTest extends FlowTestCase
{
    public function test_dom_element_attribute_value(): void
    {
        $rows = df()
            ->read(from_rows(rows(row(xml_element_entry('node', '<name id="1">User Name 01</name>')))))
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
            ->read(from_rows(rows(row(xml_entry('node', '<name id="1">User Name 01</name>')))))
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
            ->read(from_rows(rows(row(xml_entry('node', '<user><name id="1">User Name 01</name></user>')))))
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

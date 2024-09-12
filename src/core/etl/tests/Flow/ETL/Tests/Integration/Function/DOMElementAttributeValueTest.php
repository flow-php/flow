<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_rows, ref, row, rows, xml_element_entry, xml_entry};
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;

final class DOMElementAttributeValueTest extends TestCase
{
    public function test_dom_element_attribute_value() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(
                        xml_element_entry('node', '<name id="1">User Name 01</name>')
                    )
                )
            ))
            ->withEntry('user_id', ref('node')->domElementAttributeValue('id'))
            ->drop('node')
            ->fetch();

        self::assertSame(
            [
                ['user_id' => '1'],
            ],
            $rows->toArray()
        );
    }

    public function test_dom_element_attribute_value_from_dom_document() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(
                        xml_entry('node', '<name id="1">User Name 01</name>')
                    )
                )
            ))
            ->withEntry('user_id', ref('node')->domElementAttributeValue('id'))
            ->drop('node')
            ->fetch();

        self::assertSame(
            [
                ['user_id' => '1'],
            ],
            $rows->toArray()
        );
    }

    public function test_dom_element_attribute_value_on_xpath_result() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(
                        xml_entry('node', '<user><name id="1">User Name 01</name></user>')
                    )
                )
            ))
            ->withEntry('user_id', ref('node')->xpath('name')->domElementAttributeValue('id'))
            ->drop('node')
            ->fetch();

        self::assertSame(
            [
                ['user_id' => '1'],
            ],
            $rows->toArray()
        );
    }
}

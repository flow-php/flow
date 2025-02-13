<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_rows, ref, row, rows, type_string, xml_element_entry, xml_entry};

use Flow\ETL\Tests\FlowTestCase;

final class DOMElementValueTest extends FlowTestCase
{
    public function test_dom_element_cast_as_string() : void
    {
        $document = new \DOMDocument();
        $document->loadXml('<b>User Name 01</b>');

        $rows = df()
            ->read(from_rows(
                rows(
                    row(
                        xml_entry('html_raw', $document)
                    )
                )
            ))
            ->withEntry('html', ref('html_raw')->cast(type_string()))
            ->drop('html_raw')
            ->fetch();

        self::assertSame(
            [
                ['html' => '<b>User Name 01</b>'],
            ],
            $rows->toArray()
        );
    }

    public function test_dom_element_value() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(
                        xml_element_entry('node', '<name>User Name 01</name>')
                    )
                )
            ))
            ->withEntry('user_name', ref('node')->domElementValue())
            ->drop('node')
            ->fetch();

        self::assertSame(
            [
                ['user_name' => 'User Name 01'],
            ],
            $rows->toArray()
        );
    }

    public function test_dom_element_value_from_dom_document() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(
                        xml_entry('node', '<name>User Name 01</name>')
                    )
                )
            ))
            ->withEntry('user_name', ref('node')->domElementValue())
            ->drop('node')
            ->fetch();

        self::assertSame(
            [
                ['user_name' => 'User Name 01'],
            ],
            $rows->toArray()
        );
    }

    public function test_dom_element_value_on_dom_document() : void
    {
        $document = new \DOMDocument();
        $document->loadXml('<b>User Name 01</b>');

        $rows = df()
            ->read(from_rows(
                rows(
                    row(
                        xml_entry('html_raw', $document)
                    )
                )
            ))
            ->withEntry('html', ref('html_raw')->domElementValue())
            ->drop('html_raw')
            ->fetch();

        self::assertSame(
            [
                ['html' => 'User Name 01'],
            ],
            $rows->toArray()
        );
    }

    public function test_dom_element_value_on_xpath_result() : void
    {
        $rows = df()
            ->read(from_rows(
                rows(
                    row(
                        xml_entry('node', '<user><name>User Name 01</name></user>')
                    )
                )
            ))
            ->withEntry('user_name', ref('node')->xpath('name')->domElementValue())
            ->drop('node')
            ->fetch();

        self::assertSame(
            [
                ['user_name' => 'User Name 01'],
            ],
            $rows->toArray()
        );
    }
}

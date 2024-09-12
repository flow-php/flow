<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_rows, ref, row, rows, xml_element_entry, xml_entry};
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;

final class DOMElementValueTest extends TestCase
{
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

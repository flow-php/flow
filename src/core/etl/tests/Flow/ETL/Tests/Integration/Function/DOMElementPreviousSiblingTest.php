<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use DOMDocument;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\html_element_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\xml_element_entry;

final class DOMElementPreviousSiblingTest extends FlowTestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_dom_element_sibling_text_value(): void
    {
        $rows = df()
            ->read(from_rows(rows(row(html_element_entry(
                'html_element',
                '<article>01<section><h1>User Name</h1></section></article>',
            )))))
            ->withEntry('user_details', ref('html_element')->htmlQuerySelector('section'))
            ->withEntry('user_name', ref('user_details')->htmlQuerySelector('h1')->domElementValue())
            ->withEntry('user_id', ref('user_details')->domElementPreviousSibling()->domElementValue())
            ->select('user_name', 'user_id')
            ->fetch();

        static::assertSame(
            [
                [
                    'user_name' => 'User Name',
                    'user_id' => '01',
                ],
            ],
            $rows->toArray(),
        );
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_dom_element_sibling_text_value_when_only_element_is_allowed(): void
    {
        $rows = df()
            ->read(from_rows(rows(row(html_element_entry(
                'html_element',
                '<article>01<section><h1>User Name</h1></section></article>',
            )))))
            ->withEntry('user_details', ref('html_element')->htmlQuerySelector('section'))
            ->withEntry('user_name', ref('user_details')->htmlQuerySelector('h1')->domElementValue())
            ->withEntry('user_id', ref('user_details')->domElementPreviousSibling(true)->domElementValue())
            ->select('user_name', 'user_id')
            ->fetch();

        static::assertSame(
            [
                [
                    'user_name' => 'User Name',
                    'user_id' => null,
                ],
            ],
            $rows->toArray(),
        );
    }

    public function test_xml_sibling_element_value(): void
    {
        $dom = new DOMDocument();
        $dom->loadXML('<user><name>User Name</name><number>01</number></user>');

        $rows = df()
            ->read(from_rows(rows(row(xml_element_entry(
                'xml_element',
                $dom->getElementsByTagName('number')->item(0),
            )))))
            ->withEntry('user_id', ref('xml_element')->domElementValue())
            ->withEntry('user_name', ref('xml_element')->domElementPreviousSibling()->domElementValue())
            ->select('user_name', 'user_id')
            ->fetch();

        static::assertSame(
            [
                [
                    'user_name' => 'User Name',
                    'user_id' => '01',
                ],
            ],
            $rows->toArray(),
        );
    }
}

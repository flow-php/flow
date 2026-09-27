<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use DOMDocument;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\html_element_schema;
use function Flow\ETL\DSL\optional;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\type_html_element;

final class DOMElementPreviousSiblingTest extends FlowTestCase
{
    #[RequiresPhp('>= 8.4.0')]
    public function test_dom_element_sibling_text_value(): void
    {
        $rows = df()
            ->read(from_rows(array_to_rows([[
                'html_element' => type_html_element()->cast(
                    '<article>01<section><h1>User Name</h1></section></article>',
                ),
            ]], schema(html_element_schema('html_element')))))
            ->withEntry('user_details', ref('html_element')->htmlQuerySelector('section'))
            ->withEntry('user_name', ref('user_details')->htmlQuerySelector('h1')->domElementValue())
            ->withEntry('user_id', optional(ref('user_details')->domElementPreviousSibling()->domElementValue()))
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

    #[RequiresPhp('>= 8.4.0')]
    public function test_dom_element_sibling_text_value_when_only_element_is_allowed(): void
    {
        $rows = df()
            ->read(from_rows(array_to_rows([[
                'html_element' => type_html_element()->cast(
                    '<article>01<section><h1>User Name</h1></section></article>',
                ),
            ]], schema(html_element_schema('html_element')))))
            ->withEntry('user_details', ref('html_element')->htmlQuerySelector('section'))
            ->withEntry('user_name', ref('user_details')->htmlQuerySelector('h1')->domElementValue())
            ->withEntry('user_id', optional(ref('user_details')->domElementPreviousSibling()->domElementValue()))
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

        // a batch stores an element's own markup, so siblings are reached from the document it belongs to
        $rows = df()
            ->read(from_rows(array_to_rows([['xml' => $dom]], schema(xml_schema('xml')))))
            ->withEntry('user_id', ref('xml')->xpath('/user/number')->arrayGet('0')->domElementValue())
            ->withEntry(
                'user_name',
                ref('xml')->xpath('/user/number')->arrayGet('0')->domElementPreviousSibling()->domElementValue(),
            )
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

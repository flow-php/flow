<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_rows, html_entry, ref, row, rows};
use Dom\HTMLDocument;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

#[RequiresPhp('>= 8.4')]
final class HTMLQuerySelectorTest extends TestCase
{
    public function test_invalid_query_on_html_document() : void
    {
        $html = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div><span>foobar</span></div></body></html>');

        self::assertEquals(
            [
                [
                    'html_element' => null,
                ],
            ],
            df()
                ->read(from_rows(rows(row(html_entry('html_raw', $html)))))
                ->withEntry('html_element', ref('html_raw')->htmlQuerySelector('body div p'))
                ->drop('html_raw')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_valid_query_on_html_document() : void
    {
        $html = HTMLDocument::createFromString(
            <<<'HTML'
<!DOCTYPE html>
<html lang="en">
<head></head>
<body>
<div><span>foo</span></div>
<div><p>bar</p></div>
<div><p>baz</p></div>
</body>
</html>
HTML
        );

        self::assertEquals(
            [
                [
                    'span_element' => 'foo',
                    'p_element' => null,
                ],
                [
                    'span_element' => null,
                    'p_element' => 'bar',
                ],
                [
                    'span_element' => null,
                    'p_element' => 'baz',
                ],
            ],
            df()
                ->read(from_rows(rows(row(html_entry('html_raw', $html)))))
                ->withEntry('containers', ref('html_raw')->htmlQuerySelectorAll('body div')->expand())
                ->withEntry('span_element', ref('containers')->htmlQuerySelector('body span')->domElementValue())
                ->withEntry('p_element', ref('containers')->htmlQuerySelector('p')->domElementValue())
                ->select('span_element', 'p_element')
                ->fetch()
                ->toArray()
        );
    }
}

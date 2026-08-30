<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use Dom\HTMLDocument;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

use const LIBXML_HTML_NOIMPLIED;
use const LIBXML_NOERROR;

// @mago-ignore analysis:unavailable-method
#[RequiresPhp('>= 8.4.0')]
final class HTMLQuerySelectorAllTest extends TestCase
{
    public function test_invalid_query_all_on_html_document(): void
    {
        $html = HTMLDocument::createFromString(
            '<!DOCTYPE html><html lang="en"><head></head><body><div><span>foobar</span></div></body></html>',
        );

        static::assertEquals(
            [
                [
                    'html' => null,
                ],
            ],
            df()
                ->read(from_rows(rows(schema(html_schema('html_raw')), row(['html_raw' => $html]))))
                ->withEntry('html', ref('html_raw')->htmlQuerySelectorAll('body div p'))
                ->drop('html_raw')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_valid_query_all_on_html_document(): void
    {
        $html = HTMLDocument::createFromString(
            '<!DOCTYPE html><html lang="en"><head></head><body><div><span>foo</span><span>bar</span></div></body></html>',
        );

        $elementFoo = HTMLDocument::createFromString('<span>foo</span>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);
        $elementBar = HTMLDocument::createFromString('<span>bar</span>', LIBXML_HTML_NOIMPLIED | LIBXML_NOERROR);

        static::assertEquals(
            [
                [
                    'html' => [
                        $elementFoo->documentElement,
                        $elementBar->documentElement,
                    ],
                ],
            ],
            df()
                ->read(from_rows(rows(schema(html_schema('html_raw')), row(['html_raw' => $html]))))
                ->withEntry('html', ref('html_raw')->htmlQuerySelectorAll('body div span'))
                ->drop('html_raw')
                ->fetch()
                ->toArray(),
        );
    }
}

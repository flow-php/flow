<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_rows, html_entry, ref, row, rows};
use Dom\HTMLDocument;
use PHPUnit\Framework\Attributes\RequiresPhp;
use PHPUnit\Framework\TestCase;

#[RequiresPhp('>= 8.4')]
final class HTMLQuerySelectorAllTest extends TestCase
{
    public function test_invalid_query_all_on_html_document() : void
    {
        $html = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div><span>foobar</span></div></body></html>');

        self::assertEquals(
            [
                [
                    'html' => null,
                ],
            ],
            df()
                ->read(from_rows(rows(row(html_entry('html_raw', $html)))))
                ->withEntry('html', ref('html_raw')->htmlQuerySelectorAll('body div p'))
                ->drop('html_raw')
                ->fetch()
                ->toArray(),
        );
    }

    public function test_valid_query_all_on_html_document() : void
    {
        $html = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div><span>foo</span><span>bar</span></div></body></html>');

        $elementFoo = HTMLDocument::createFromString('<span>foo</span>', \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);
        $elementBar = HTMLDocument::createFromString('<span>bar</span>', \LIBXML_HTML_NOIMPLIED | \LIBXML_NOERROR);

        self::assertEquals(
            [
                [
                    'html' => [
                        $elementFoo->documentElement,
                        $elementBar->documentElement,
                    ],
                ],
            ],
            df()
                ->read(from_rows(rows(row(html_entry('html_raw', $html)))))
                ->withEntry('html', ref('html_raw')->htmlQuerySelectorAll('body div span'))
                ->drop('html_raw')
                ->fetch()
                ->toArray()
        );
    }
}

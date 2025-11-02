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
        /* @phpstan-ignore-next-line */
        $html = HTMLDocument::createFromString('<!DOCTYPE html><html lang="en"><head></head><body><div><span>foobar</span></div></body></html>');

        $rows = df()
            ->read(from_rows(rows(row(html_entry('html_raw', $html)))))
            ->withEntry('html', ref('html_raw')->htmlQuerySelector('body div p'))
            ->drop('html_raw')
            ->fetch();

        $results = $rows->toArray()[0]['html'] ?? [];

        /* @phpstan-ignore-next-line */
        self::assertCount(0, $results);
    }
}

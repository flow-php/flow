<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination;

use Flow\ETL\Adapter\Http\Pagination\LinkHeaderParser;
use Flow\ETL\Tests\FlowTestCase;

final class LinkHeaderParserTest extends FlowTestCase
{
    public function test_malformed_header_yields_no_match(): void
    {
        static::assertNull((new LinkHeaderParser())->next(['this is not a link header'], 'next'));
    }

    public function test_missing_rel_returns_null(): void
    {
        static::assertNull((new LinkHeaderParser())->next(
            ['<https://api.example.com/items?page=2>; rel="prev"'],
            'next',
        ));
    }

    public function test_multiple_headers(): void
    {
        static::assertSame('https://api.example.com/items?page=2', (new LinkHeaderParser())->next(
            [
                '<https://api.example.com/items?page=9>; rel="last"',
                '<https://api.example.com/items?page=2>; rel="next"',
            ],
            'next',
        ));
    }

    public function test_multiple_links_in_one_header(): void
    {
        static::assertSame('https://api.example.com/items?page=3', (new LinkHeaderParser())->next([
            '<https://api.example.com/items?page=1>; rel="prev", <https://api.example.com/items?page=3>; rel="next", <https://api.example.com/items?page=9>; rel="last"',
        ], 'next'));
    }

    public function test_space_separated_rel_types(): void
    {
        static::assertSame('https://api.example.com/items?page=2', (new LinkHeaderParser())->next(
            ['<https://api.example.com/items?page=2>; rel="next page"'],
            'next',
        ));
    }

    public function test_unquoted_rel(): void
    {
        static::assertSame('https://api.example.com/items?page=2', (new LinkHeaderParser())->next(
            ['<https://api.example.com/items?page=2>; rel=next'],
            'next',
        ));
    }
}

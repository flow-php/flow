<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination\Strategy;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\LinkHeader;
use Flow\ETL\Adapter\Http\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class LinkHeaderTest extends FlowTestCase
{
    public function test_missing_rel_stops(): void
    {
        static::assertNull((new LinkHeader())->nextRequest(
            PaginationMother::request('GET', 'https://api.example.com/items'),
            PaginationMother::decoded(['data' => [1]], [
                'Link' => '<https://api.example.com/items?page=1>; rel="prev"',
            ]),
            new PageState(pagesFetched: 1),
        ));
    }

    public function test_next_request_follows_link_header(): void
    {
        static::assertSame(
            'https://api.example.com/items?page=2',
            (new LinkHeader())
                ->nextRequest(
                    PaginationMother::request('GET', 'https://api.example.com/items'),
                    PaginationMother::decoded(['data' => [1]], [
                        'Link' => '<https://api.example.com/items?page=2>; rel="next"',
                    ]),
                    new PageState(pagesFetched: 1),
                )
                ?->getUri()
                ->__toString(),
        );
    }
}

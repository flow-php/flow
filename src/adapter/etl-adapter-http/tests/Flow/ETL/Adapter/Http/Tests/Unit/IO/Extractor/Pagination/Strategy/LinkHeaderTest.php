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

    public function test_next_request_removes_custom_sensitive_headers_on_cross_host(): void
    {
        $result = (new LinkHeader('next', ['X-Api-Key']))->nextRequest(
            PaginationMother::request('GET', 'https://api.example.com/items')
                ->withHeader('Authorization', 'Bearer secret')
                ->withHeader('X-Api-Key', 'key'),
            PaginationMother::decoded(['data' => [1]], [
                'Link' => '<https://other.example.org/items?page=2>; rel="next"',
            ]),
            new PageState(pagesFetched: 1),
        );

        static::assertNotNull($result);
        static::assertSame('other.example.org', $result->getUri()->getHost());
        static::assertFalse($result->hasHeader('Authorization'));
        static::assertFalse($result->hasHeader('X-Api-Key'));
    }
}

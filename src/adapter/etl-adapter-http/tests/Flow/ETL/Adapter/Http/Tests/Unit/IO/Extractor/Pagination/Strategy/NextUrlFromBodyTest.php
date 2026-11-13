<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination\Strategy;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\Strategy\NextUrlFromBody;
use Flow\ETL\Adapter\Http\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class NextUrlFromBodyTest extends FlowTestCase
{
    public function test_missing_url_stops(): void
    {
        static::assertNull((new NextUrlFromBody('@odata\.nextLink'))->nextRequest(
            PaginationMother::request(),
            PaginationMother::decoded(['value' => [1]]),
            new PageState(pagesFetched: 1),
        ));
    }

    public function test_next_request_replaces_uri_with_body_url(): void
    {
        static::assertSame(
            'https://api.example.com/items?page=2',
            (new NextUrlFromBody('@odata\.nextLink'))
                ->nextRequest(
                    PaginationMother::request('GET', 'https://api.example.com/items?page=1'),
                    PaginationMother::decoded(['@odata.nextLink' => 'https://api.example.com/items?page=2']),
                    new PageState(pagesFetched: 1),
                )
                ?->getUri()
                ->__toString(),
        );
    }

    public function test_next_request_removes_custom_sensitive_headers_on_cross_host(): void
    {
        $result = (new NextUrlFromBody('@odata\\.nextLink', ['X-Api-Key']))->nextRequest(
            PaginationMother::request('GET', 'https://api.example.com/items?page=1')
                ->withHeader('Authorization', 'Bearer secret')
                ->withHeader('X-Api-Key', 'key'),
            PaginationMother::decoded(['@odata.nextLink' => 'https://other.example.org/items?page=2']),
            new PageState(pagesFetched: 1),
        );

        static::assertNotNull($result);
        static::assertSame('other.example.org', $result->getUri()->getHost());
        static::assertFalse($result->hasHeader('Authorization'));
        static::assertFalse($result->hasHeader('X-Api-Key'));
    }
}

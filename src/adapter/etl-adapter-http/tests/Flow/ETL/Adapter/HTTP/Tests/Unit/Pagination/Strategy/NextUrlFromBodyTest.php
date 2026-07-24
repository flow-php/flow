<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\Strategy;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\Strategy\NextUrlFromBody;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
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
}

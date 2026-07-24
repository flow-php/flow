<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination;

use Flow\ETL\Adapter\Http\Pagination\RequestOption;
use Flow\ETL\Tests\FlowTestCase;
use Nyholm\Psr7\Factory\Psr17Factory;

use function json_decode;
use function parse_str;

final class RequestOptionTest extends FlowTestCase
{
    public function test_body_path_sets_cursor_on_json_post_body(): void
    {
        $factory = new Psr17Factory();
        $request = $factory
            ->createRequest('POST', 'https://api.example.com/search')
            ->withHeader('Content-Type', 'application/json')
            ->withBody($factory->createStream('{"filter":{"active":true}}'));

        $result = RequestOption::bodyPath('cursor', $factory)->apply($request, 'abc');

        static::assertSame(
            ['filter' => ['active' => true], 'cursor' => 'abc'],
            json_decode((string) $result->getBody(), true, 512, JSON_THROW_ON_ERROR),
        );
    }

    public function test_header_replaces_value(): void
    {
        $request = (new Psr17Factory())
            ->createRequest('GET', 'https://api.example.com/items')
            ->withHeader('X-Cursor', 'old');

        static::assertSame(['new'], RequestOption::header('X-Cursor')->apply($request, 'new')->getHeader('X-Cursor'));
    }

    public function test_query_param_merges_and_keeps_other_params(): void
    {
        $request = (new Psr17Factory())->createRequest('GET', 'https://api.example.com/items?per_page=50');

        $query = [];
        parse_str(RequestOption::queryParam('page')->apply($request, 2)->getUri()->getQuery(), $query);

        static::assertSame(['per_page' => '50', 'page' => '2'], $query);
    }

    public function test_query_param_overrides_same_name(): void
    {
        $request = (new Psr17Factory())->createRequest('GET', 'https://api.example.com/items?page=1&per_page=50');

        $query = [];
        parse_str(RequestOption::queryParam('page')->apply($request, 7)->getUri()->getQuery(), $query);

        static::assertSame(['page' => '7', 'per_page' => '50'], $query);
    }

    public function test_replace_uri_resolves_relative_reference_against_base(): void
    {
        $request = (new Psr17Factory())->createRequest('GET', 'https://api.example.com/v2/items?page=1');

        static::assertSame(
            'https://api.example.com/v2/items?page=2',
            (string) RequestOption::replaceUri()->apply($request, '?page=2')->getUri(),
        );
    }
}

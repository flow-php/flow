<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\RequestOption;
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

    public function test_replace_uri_keeps_sensitive_headers_on_same_host(): void
    {
        $request = (new Psr17Factory())
            ->createRequest('GET', 'https://api.example.com/items?page=1')
            ->withHeader('Authorization', 'Bearer secret')
            ->withHeader('Cookie', 'session=abc');

        $result = RequestOption::replaceUri()->apply($request, '/items?page=2');

        static::assertSame(['Bearer secret'], $result->getHeader('Authorization'));
        static::assertSame(['session=abc'], $result->getHeader('Cookie'));
    }

    public function test_replace_uri_removes_sensitive_headers_on_cross_host(): void
    {
        $request = (new Psr17Factory())
            ->createRequest('GET', 'https://api.example.com/items?page=1')
            ->withHeader('Authorization', 'Bearer secret')
            ->withHeader('Proxy-Authorization', 'Basic secret')
            ->withHeader('Cookie', 'session=abc')
            ->withHeader('Accept', 'application/json');

        $result = RequestOption::replaceUri()->apply($request, 'https://evil.example.org/items?page=2');

        static::assertSame('evil.example.org', $result->getUri()->getHost());
        static::assertFalse($result->hasHeader('Authorization'));
        static::assertFalse($result->hasHeader('Proxy-Authorization'));
        static::assertFalse($result->hasHeader('Cookie'));
        static::assertSame(['application/json'], $result->getHeader('Accept'));
    }

    public function test_replace_uri_on_cross_host_without_sensitive_headers(): void
    {
        $request = (new Psr17Factory())->createRequest('GET', 'https://api.example.com/items?page=1');

        $result = RequestOption::replaceUri()->apply($request, 'https://other.example.org/items?page=2');

        static::assertSame('https://other.example.org/items?page=2', (string) $result->getUri());
        static::assertFalse($result->hasHeader('Authorization'));
        static::assertFalse($result->hasHeader('Cookie'));
    }

    public function test_replace_uri_removes_custom_sensitive_headers_on_cross_host(): void
    {
        $request = (new Psr17Factory())
            ->createRequest('GET', 'https://api.example.com/items?page=1')
            ->withHeader('Authorization', 'Bearer secret')
            ->withHeader('X-Api-Key', 'key')
            ->withHeader('Accept', 'application/json');

        $result = RequestOption::replaceUri(['x-api-key'])->apply($request, 'https://evil.example.org/items?page=2');

        static::assertFalse($result->hasHeader('Authorization'));
        static::assertFalse($result->hasHeader('X-Api-Key'));
        static::assertSame(['application/json'], $result->getHeader('Accept'));
    }

    public function test_replace_uri_keeps_custom_sensitive_headers_on_same_host(): void
    {
        $request = (new Psr17Factory())
            ->createRequest('GET', 'https://api.example.com/items?page=1')
            ->withHeader('X-Api-Key', 'key');

        $result = RequestOption::replaceUri(['X-Api-Key'])->apply($request, '/items?page=2');

        static::assertSame(['key'], $result->getHeader('X-Api-Key'));
    }

    public function test_replace_uri_removes_sensitive_headers_on_scheme_downgrade(): void
    {
        $request = (new Psr17Factory())
            ->createRequest('GET', 'https://api.example.com/items?page=1')
            ->withHeader('Authorization', 'Bearer secret')
            ->withHeader('Cookie', 'session=abc');

        $result = RequestOption::replaceUri()->apply($request, 'http://api.example.com/items?page=2');

        static::assertSame('http', $result->getUri()->getScheme());
        static::assertFalse($result->hasHeader('Authorization'));
        static::assertFalse($result->hasHeader('Cookie'));
    }

    public function test_replace_uri_keeps_sensitive_headers_on_scheme_upgrade(): void
    {
        $request = (new Psr17Factory())
            ->createRequest('GET', 'http://api.example.com/items?page=1')
            ->withHeader('Authorization', 'Bearer secret')
            ->withHeader('Cookie', 'session=abc');

        $result = RequestOption::replaceUri()->apply($request, 'https://api.example.com/items?page=2');

        static::assertSame('https', $result->getUri()->getScheme());
        static::assertSame(['Bearer secret'], $result->getHeader('Authorization'));
        static::assertSame(['session=abc'], $result->getHeader('Cookie'));
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

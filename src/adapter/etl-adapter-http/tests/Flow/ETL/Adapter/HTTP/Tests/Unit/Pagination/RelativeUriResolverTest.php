<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination;

use Flow\ETL\Adapter\Http\Pagination\RelativeUriResolver;
use Flow\ETL\Tests\FlowTestCase;
use Nyholm\Psr7\Uri;
use PHPUnit\Framework\Attributes\DataProvider;

final class RelativeUriResolverTest extends FlowTestCase
{
    public static function references(): array
    {
        return [
            'absolute url' => [
                'https://api.example.com/v2/orgs/flow-php/repos?page=2',
                'https://other.example.com/next?cursor=abc',
                'https://other.example.com/next?cursor=abc',
            ],
            'absolute path' => [
                'https://api.example.com/v2/orgs/flow-php/repos?page=2',
                '/v2/orgs/flow-php/repos?page=3',
                'https://api.example.com/v2/orgs/flow-php/repos?page=3',
            ],
            'relative path' => [
                'https://api.example.com/v2/orgs/flow-php/repos?page=2',
                'members?page=3',
                'https://api.example.com/v2/orgs/flow-php/members?page=3',
            ],
            'query only' => [
                'https://api.example.com/v2/items?page=2&per_page=50',
                '?page=3',
                'https://api.example.com/v2/items?page=3',
            ],
        ];
    }

    #[DataProvider('references')]
    public function test_resolution(string $base, string $reference, string $expected): void
    {
        static::assertSame($expected, (string) (new RelativeUriResolver())->resolve(new Uri($base), $reference));
    }
}

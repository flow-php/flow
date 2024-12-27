<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Unit\BlobService\URLFactory;

use Flow\Azure\SDK\BlobService\URLFactory\{AzureURLFactory, AzuriteURLFactory};
use Flow\Azure\SDK\BlobService\{Configuration};
use Flow\Azure\SDK\URLFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class URLFactoryTest extends TestCase
{
    public static function factoryProvider() : array
    {
        return [
            ['factory' => new AzureURLFactory()],
            ['factory' => new AzuriteURLFactory()],
        ];
    }

    #[DataProvider('factoryProvider')]
    public function test_creating_get_url_with_array_query_parameters(URLFactory $factory) : void
    {
        $configuration = new Configuration('account', 'container');

        $url = $factory->create($configuration, null, ['foo' => ['biz', 'bar']]);

        self::assertStringEndsWith('?foo%5B0%5D=biz&foo%5B1%5D=bar', $url);
    }

    #[DataProvider('factoryProvider')]
    public function test_creating_get_url_with_query_parameters(URLFactory $factory) : void
    {
        $configuration = new Configuration('account', 'container');

        $url = $factory->create($configuration, null, ['foo' => 'bar']);

        self::assertStringEndsWith('?foo=bar', $url);
    }

    #[DataProvider('factoryProvider')]
    public function test_creating_get_url_without_query_parameters(URLFactory $factory) : void
    {
        $configuration = new Configuration('account', 'container');

        $url = $factory->create($configuration);

        self::assertStringNotContainsString('?', $url);
    }
}

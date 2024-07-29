<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{config_builder, data_frame, filesystem_cache, from_cache, ref, to_stream};
use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message\{RequestInterface, ResponseInterface};

require __DIR__ . '/../../../autoload.php';

$factory = new Psr17Factory();
$client = new Client($factory, $factory);

$from_github_api = new PsrHttpClientDynamicExtractor($client, new class implements NextRequestFactory {
    public function create(?ResponseInterface $previousResponse = null) : ?RequestInterface
    {
        $factory = new Psr17Factory();

        if ($previousResponse === null) {
            return $factory
                ->createRequest('GET', 'https://api.github.com/orgs/flow-php')
                ->withHeader('Accept', 'application/vnd.github.v3+json')
                ->withHeader('User-Agent', 'flow-php/etl');
        }

        return null;
    }
});

data_frame(config_builder()->cache(filesystem_cache(__DIR__ . '/output/cache')))
    ->read(
        from_cache(
            id: 'github_api',
            fallback_extractor: $from_github_api
        )
    )
    ->cache('github_api')
    ->withEntry('unpacked', ref('response_body')->jsonDecode())
    ->select('unpacked')
    ->withEntry('unpacked', ref('unpacked')->unpack())
    ->renameAll('unpacked.', '')
    ->drop('unpacked')
    ->select('name', 'html_url', 'blog', 'login', 'public_repos', 'followers', 'created_at')
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();

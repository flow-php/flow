<?php

declare(strict_types=1);

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Cache\PSRSimpleCache;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Psr16Cache;

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

$adapter = new PSRSimpleCache(
    new Psr16Cache(
        new FilesystemAdapter(
            directory: __DIR__ . '/output/cache'
        )
    )
);

df(config_builder()->cache($adapter))
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
    ->write(to_output(false))
    ->run();

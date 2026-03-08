<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, ref, rename_replace, to_output};
use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Symfony\Component\HttpClient\Psr18Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message\{RequestInterface, ResponseInterface};

require __DIR__ . '/vendor/autoload.php';

$factory = new Psr17Factory();
$client = new Psr18Client();

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

data_frame()
    ->read($from_github_api)
    ->withEntry('unpacked', ref('response_body')->jsonDecode())
    ->withEntry('unpacked', ref('unpacked')->unpack())
    ->renameEach(rename_replace('unpacked.', ''))
    ->drop('unpacked')
    ->select('name', 'html_url', 'blog', 'login', 'public_repos', 'followers', 'created_at')
    ->write(to_output(truncate: false))
    ->run();

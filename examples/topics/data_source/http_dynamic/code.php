<?php

declare(strict_types=1);

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_output;
use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message;

require __DIR__ . '/../../../autoload.php';

$factory = new Psr17Factory();
$client = new Client($factory, $factory);

$from_github_api = new PsrHttpClientDynamicExtractor($client, new class implements NextRequestFactory {
    public function create(?Message\ResponseInterface $previousResponse = null) : ?Message\RequestInterface
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
    ->select('unpacked')
    ->withEntry('unpacked', ref('unpacked')->unpack())
    ->renameAll('unpacked.', '')
    ->drop('unpacked')
    ->select('name', 'html_url', 'blog', 'login', 'public_repos', 'followers', 'created_at')
    ->write(to_output(false))
    ->run();

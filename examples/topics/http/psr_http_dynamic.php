<?php

declare(strict_types=1);

use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\DSL\To;
use Flow\ETL\Flow;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message;

$factory = new Psr17Factory();
$client = new Client($factory, $factory);

$extractor = new PsrHttpClientDynamicExtractor($client, new class implements NextRequestFactory {
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

return (new Flow())
    ->read($extractor)
    ->withEntry('unpacked', ref('response_body')->jsonDecode())
    ->select('unpacked')
    ->withEntry('unpacked', ref('unpacked')->unpack())
    ->renameAll('unpacked.', '')
    ->select('name', 'html_url', 'blog')
    ->write(To::output(false));

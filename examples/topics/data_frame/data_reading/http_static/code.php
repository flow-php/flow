<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, ref, to_stream};
use Flow\ETL\Adapter\Http\PsrHttpClientStaticExtractor;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\HttpClient\{MockHttpClient, Psr18Client};
use Symfony\Component\HttpClient\Response\MockResponse;

require __DIR__ . '/vendor/autoload.php';

$requests = static function () : \Generator {
    yield (new Psr17Factory())
        ->createRequest('GET', 'https://example.com');
};

$client = new PsrHttpClientStaticExtractor(
    new Psr18Client(
        new MockHttpClient(
            [
                new MockResponse(
                    file_get_contents(__DIR__ . '/input/example.com.html'),
                    [
                        'response_headers' => [
                            'Content-Type' => 'text/html',
                        ],
                    ],
                ),
            ],
        ),
    ),
    $requests(),
);

data_frame()
    ->read($client)
    ->withEntry('title', ref('response_body')->htmlQuerySelector('body div h1')->domElementValue())
    ->withEntry('paragraphs', ref('response_body')->htmlQuerySelectorAll('body p')->expand())
    ->select('title', 'paragraphs')
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();

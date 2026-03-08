<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{data_frame, ref, to_output};
use Flow\ETL\Adapter\Http\PsrHttpClientStaticExtractor;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Component\HttpClient\{MockHttpClient, Psr18Client};
use Symfony\Component\HttpClient\Response\MockResponse;
use function Flow\Filesystem\DSL\fstab;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\protocol;

require __DIR__ . '/vendor/autoload.php';

$fs = fstab()->for(protocol('file'));

$htmlContent = $fs->readFrom(path(__DIR__ . '/input/example.com.html'))?->content();

if ($htmlContent === null) {
    print 'Example skipped. Could not read input file.' . PHP_EOL;

    return;
}

$requests = static function () : \Generator {
    yield (new Psr17Factory())
        ->createRequest('GET', 'https://example.com');
};

$client = new PsrHttpClientStaticExtractor(
    new Psr18Client(
        new MockHttpClient(
            [
                new MockResponse(
                    $htmlContent,
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
    ->write(to_output(truncate: false))
    ->run();

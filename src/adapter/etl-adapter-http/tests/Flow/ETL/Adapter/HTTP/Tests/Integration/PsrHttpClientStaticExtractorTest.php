<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Integration;

use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\Adapter\Http\PsrHttpClientStaticExtractor;
use Flow\ETL\{Rows, Tests\FlowTestCase};
use Http\Mock\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;

final class PsrHttpClientStaticExtractorTest extends FlowTestCase
{
    public function test_http_extractor() : void
    {
        $psr17Factory = new Psr17Factory();
        $psr18Client = new Client($psr17Factory);
        $norbertFixture = \file_get_contents(__DIR__ . '/../Fixtures/norberttech.json');

        if ($norbertFixture === false) {
            throw new \RuntimeException('Failed to read norberttech fixture file');
        }

        $tomaszFixture = \file_get_contents(__DIR__ . '/../Fixtures/tomaszhanc.json');

        if ($tomaszFixture === false) {
            throw new \RuntimeException('Failed to read tomaszhanc fixture file');
        }

        $psr18Client->addResponse(
            new Response(200, [], $norbertFixture),
        );
        $psr18Client->addResponse(
            new Response(200, [], $tomaszFixture),
        );

        $requests = static function () use ($psr17Factory) : \Generator {
            yield $psr17Factory
                ->createRequest('GET', 'https://api.github.com/users/norberttech')
                ->withHeader('Accept', 'application/vnd.github.v3+json')
                ->withHeader('User-Agent', 'flow-php/etl');

            yield $psr17Factory
                ->createRequest('GET', 'https://api.github.com/users/tomaszhanc')
                ->withHeader('Accept', 'application/vnd.github.v3+json')
                ->withHeader('User-Agent', 'flow-php/etl');
        };

        $extractor = new PsrHttpClientStaticExtractor($psr18Client, $requests());

        $rowsGenerator = $extractor->extract(flow_context(config()));

        /** @var Rows $norbertRows */
        $norbertRows = $rowsGenerator->current();

        $rowsGenerator->next();

        /** @var Rows $tomekRows */
        $tomekRows = $rowsGenerator->current();

        $norbertResponseBodyValue = $norbertRows->first()->valueOf('response_body');
        $norbertBodyJson = \is_scalar($norbertResponseBodyValue) || $norbertResponseBodyValue instanceof \Stringable ? (string) $norbertResponseBodyValue : '';
        $norbertResponseBody = \json_decode($norbertBodyJson, true, 512, JSON_THROW_ON_ERROR);
        \assert(\is_array($norbertResponseBody));

        $tomekResponseBodyValue = $tomekRows->first()->valueOf('response_body');
        $tomekBodyJson = \is_scalar($tomekResponseBodyValue) || $tomekResponseBodyValue instanceof \Stringable ? (string) $tomekResponseBodyValue : '';
        $tomekResponseBody = \json_decode($tomekBodyJson, true, 512, JSON_THROW_ON_ERROR);
        \assert(\is_array($tomekResponseBody));

        self::assertSame('norberttech', $norbertResponseBody['login']);
        self::assertSame('tomaszhanc', $tomekResponseBody['login']);
    }
}

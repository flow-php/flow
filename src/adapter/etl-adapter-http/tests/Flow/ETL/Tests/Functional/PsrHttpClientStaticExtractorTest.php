<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Functional;

use Flow\ETL\Adapter\Http\PsrHttpClientStaticExtractor;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Http\Mock\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\TestCase;

final class PsrHttpClientStaticExtractorTest extends TestCase
{
    public function test_http_extractor() : void
    {
        $psr17Factory = new Psr17Factory();
        $psr18Client = new Client($psr17Factory);
        $psr18Client->addResponse(
            new Response(200, [], \file_get_contents(__DIR__ . '/../json/norberttech.json')),
        );
        $psr18Client->addResponse(
            new Response(200, [], \file_get_contents(__DIR__ . '/../json/tomaszhanc.json')),
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

        $rowsGenerator = $extractor->extract(new FlowContext(Config::default()));

        /** @var Rows $norbertRows */
        $norbertRows = $rowsGenerator->current();

        $rowsGenerator->next();

        /** @var Rows $tomekRows */
        $tomekRows = $rowsGenerator->current();

        $norbertResponseBody = \json_decode($norbertRows->first()->valueOf('response_body'), true, 512, JSON_THROW_ON_ERROR);
        $tomekResponseBody = \json_decode($tomekRows->first()->valueOf('response_body'), true, 512, JSON_THROW_ON_ERROR);

        $this->assertSame('norberttech', $norbertResponseBody['login']);
        $this->assertSame('tomaszhanc', $tomekResponseBody['login']);
    }
}

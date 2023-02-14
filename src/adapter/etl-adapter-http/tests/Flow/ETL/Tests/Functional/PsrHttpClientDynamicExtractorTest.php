<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Functional;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Nyholm\Psr7\Factory\Psr17Factory;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class PsrHttpClientDynamicExtractorTest extends TestCase
{
    public function test_http_extractor() : void
    {
        $psr17Factory = new Psr17Factory();
        $psr18Client = new \Http\Mock\Client($psr17Factory);

        $psr18Client->addResponse(
            new Response(200, [
                'Server' => 'GitHub.com',
            ], \file_get_contents(__DIR__ . '/../json/flow-php.json')),
        );

        $extractor = new PsrHttpClientDynamicExtractor($psr18Client, new class implements NextRequestFactory {
            public function create(?ResponseInterface $previousResponse = null) : ?RequestInterface
            {
                $psr17Factory = new Psr17Factory();

                if ($previousResponse === null) {
                    return $psr17Factory
                        ->createRequest('GET', 'https://api.github.com/orgs/flow-php')
                        ->withHeader('Accept', 'application/vnd.github.v3+json')
                        ->withHeader('User-Agent', 'flow-php/etl');
                }

                return null;
            }
        });

        $rows = $extractor->extract(new FlowContext(Config::default()));

        $body = \json_decode($rows->current()->first()->valueOf('response_body'), true, 512, JSON_THROW_ON_ERROR);

        $this->assertSame(1, $rows->current()->count());
        $this->assertSame('flow-php', $body['login'], \json_encode($body, JSON_THROW_ON_ERROR));
        $this->assertSame(73_495_297, $body['id'], \json_encode($body, JSON_THROW_ON_ERROR));
        $this->assertSame(['GitHub.com'], $rows->current()->first()->valueOf('response_headers')['Server']);
        $this->assertSame(200, $rows->current()->first()->valueOf('response_status_code'));
        $this->assertSame('1.1', $rows->current()->first()->valueOf('response_protocol_version'));
        $this->assertSame('OK', $rows->current()->first()->valueOf('response_reason_phrase'));
        $this->assertSame('https://api.github.com/orgs/flow-php', $rows->current()->first()->valueOf('request_uri'));
        $this->assertSame('GET', $rows->current()->first()->valueOf('request_method'));
    }
}

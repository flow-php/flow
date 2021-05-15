<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Functional;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class PsrHttpClientDynamicExtractorTest extends TestCase
{
    public function test_http_extractor() : void
    {
        $psr17Factory = new Psr17Factory();
        $psr18Client = new Client($psr17Factory, $psr17Factory);

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

        $rows = $extractor->extract();

        $body = \json_decode($rows->current()->first()->valueOf('body'), true, 512, JSON_THROW_ON_ERROR);

        $this->assertSame(1, $rows->current()->count());
        $this->assertSame('flow-php', $body['login']);
        $this->assertSame(73495297, $body['id']);
        $this->assertSame(['GitHub.com'], $rows->current()->first()->valueOf('headers')['Server']);
        $this->assertSame(200, $rows->current()->first()->valueOf('status_code'));
        $this->assertSame('1.1', $rows->current()->first()->valueOf('protocol_version'));
        $this->assertSame('OK', $rows->current()->first()->valueOf('reason_phrase'));
    }
}

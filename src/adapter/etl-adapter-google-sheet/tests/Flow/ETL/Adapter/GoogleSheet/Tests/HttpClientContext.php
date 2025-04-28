<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests;

use GuzzleHttp\{Client as HttpClient, Handler\MockHandler, Psr7\Request};
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Assert;

final class HttpClientContext
{
    public function __construct(private array $queue = [])
    {
    }

    public function add(
        HttpRequestContext $requestContext,
        string $fixtureFile,
    ) : self {
        $this->queue[] = function (Request $request) use ($requestContext, $fixtureFile) : Response {
            Assert::assertSame($requestContext->method, $request->getMethod());
            Assert::assertSame($requestContext->url, (string) $request->getUri());

            if (null !== $requestContext->body) {
                Assert::assertSame($requestContext->body, (string) $request->getBody());
            }

            return new Response(
                headers: ['Content-Type' => 'application/json'],
                body: file_get_contents($fixtureFile) ?: throw new \RuntimeException('Failed to read file: ' . $fixtureFile)
            );
        };

        return $this;
    }

    public function createHttpClient() : HttpClient
    {
        return new HttpClient(
            [
                'handler' => MockHandler::createWithMiddleware($this->queue),
            ]
        );
    }
}

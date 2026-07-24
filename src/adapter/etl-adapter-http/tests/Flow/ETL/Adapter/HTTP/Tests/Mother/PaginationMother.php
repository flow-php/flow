<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Mother;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

use function json_encode;

final class PaginationMother
{
    public static function request(
        string $method = 'GET',
        string $uri = 'https://api.example.com/items',
    ): RequestInterface {
        return (new Psr17Factory())->createRequest($method, $uri);
    }

    /**
     * @param array<mixed> $body
     * @param array<string, string> $headers
     */
    public static function jsonResponse(array $body, array $headers = [], int $status = 200): ResponseInterface
    {
        $factory = new Psr17Factory();
        $response = $factory->createResponse($status)->withHeader('Content-Type', 'application/json');

        foreach ($headers as $name => $value) {
            $response = $response->withHeader($name, $value);
        }

        return $response->withBody($factory->createStream(json_encode($body, JSON_THROW_ON_ERROR)));
    }

    /**
     * @param array<mixed> $body
     * @param array<string, string> $headers
     */
    public static function decoded(array $body, array $headers = []): DecodedResponse
    {
        return new DecodedResponse($body, self::jsonResponse($body, $headers));
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Psr\Http\Message\ResponseInterface;

final class RowsResponseFactory
{
    /**
     * @param ResponseInterface $response
     *
     * @throws InvalidArgumentException
     * @throws \JsonException
     *
     * @return Rows
     * @psalm-pure
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress InvalidLiteralArgument
     * @psalm-suppress ImpureFunctionCall
     * @psalm-suppress MixedArgument
     */
    public function create(ResponseInterface $response) : Rows
    {
        $responseType = 'html';

        foreach ($response->getHeader('Content-Type') as $header) {
            if (\strpos('application/json', $header) !== false) {
                $responseType = 'json';
            }
        }

        switch ($responseType) {
            case 'json':
                if (\class_exists('Flow\ETL\Row\Entry\JsonEntry')) {
                    $responseRow = new Row\Entry\JsonEntry('body', \json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR));
                } else {
                    $responseRow = new Row\Entry\StringEntry('body', $response->getBody()->getContents());
                }

                break;

            default:
                $responseRow = new Row\Entry\StringEntry('body', $response->getBody()->getContents());

                break;
        }

        return new Rows(
            Row::create(
                $responseRow,
                new Row\Entry\ArrayEntry('headers', $response->getHeaders()),
                new Row\Entry\IntegerEntry('status_code', $response->getStatusCode()),
                new Row\Entry\StringEntry('protocol_version', $response->getProtocolVersion()),
                new Row\Entry\StringEntry('reason_phrase', $response->getReasonPhrase()),
            )
        );
    }
}

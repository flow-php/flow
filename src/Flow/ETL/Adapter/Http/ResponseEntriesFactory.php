<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Psr\Http\Message\ResponseInterface;

final class ResponseEntriesFactory
{
    /**
     * @param ResponseInterface $response
     *
     * @throws InvalidArgumentException
     * @throws \JsonException
     *
     * @return Row\Entries
     * @psalm-pure
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress InvalidLiteralArgument
     * @psalm-suppress ImpureFunctionCall
     * @psalm-suppress MixedArgument
     */
    public function create(ResponseInterface $response) : Row\Entries
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
                    $responseBodyEntry = new Row\Entry\JsonEntry('response_body', \json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR));
                } else {
                    $responseBodyEntry = new Row\Entry\StringEntry('response_body', $response->getBody()->getContents());
                }

                break;

            default:
                $responseBodyEntry = new Row\Entry\StringEntry('response_body', $response->getBody()->getContents());

                break;
        }

        return new Row\Entries(
            $responseBodyEntry,
            new Row\Entry\ArrayEntry('response_headers', $response->getHeaders()),
            new Row\Entry\IntegerEntry('response_status_code', $response->getStatusCode()),
            new Row\Entry\StringEntry('response_protocol_version', $response->getProtocolVersion()),
            new Row\Entry\StringEntry('response_reason_phrase', $response->getReasonPhrase()),
        );
    }
}

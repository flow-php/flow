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
     *
     * @psalm-suppress InvalidLiteralArgument
     */
    public function create(ResponseInterface $response) : Row\Entries
    {
        $responseType = 'html';

        foreach ($response->getHeader('Content-Type') as $header) {
            if (\str_contains('application/json', $header)) {
                $responseType = 'json';
            }
        }

        $responseBody = $response->getBody();

        if ($responseBody->isReadable()) {
            if ($responseBody->isSeekable()) {
                $responseBody->seek(0);
            }

            $responseBodyContent = $responseBody->getContents();

            if ($responseBody->isSeekable()) {
                $responseBody->seek(0);
            }

            switch ($responseType) {
                case 'json':
                    if (\class_exists(Row\Entry\JsonEntry::class)) {
                        $responseBodyEntry = new Row\Entry\JsonEntry('response_body', (array) \json_decode($responseBodyContent, true, 512, JSON_THROW_ON_ERROR));
                    } else {
                        $responseBodyEntry = new Row\Entry\StringEntry('response_body', $responseBodyContent);
                    }

                    break;

                default:
                    $responseBodyEntry = new Row\Entry\StringEntry('response_body', $responseBodyContent);

                    break;
            }
        } else {
            $responseBodyEntry = new Row\Entry\NullEntry('response_body');
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

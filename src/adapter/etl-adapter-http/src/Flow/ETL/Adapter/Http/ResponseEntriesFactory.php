<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use function Flow\ETL\DSL\string_entry;
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
                        $responseBodyEntry = string_entry('response_body', $responseBodyContent);
                    }

                    break;

                default:
                    $responseBodyEntry = string_entry('response_body', $responseBodyContent);

                    break;
            }
        } else {
            $responseBodyEntry = string_entry('response_body', null);
        }

        return new Row\Entries(
            $responseBodyEntry,
            new Row\Entry\JsonEntry('response_headers', $response->getHeaders()),
            new Row\Entry\IntegerEntry('response_status_code', $response->getStatusCode()),
            string_entry('response_protocol_version', $response->getProtocolVersion()),
            string_entry('response_reason_phrase', $response->getReasonPhrase()),
        );
    }
}

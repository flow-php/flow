<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry\{IntegerEntry, JsonEntry};
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
    public function create(ResponseInterface $response) : Entries
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
                    if (\class_exists(JsonEntry::class)) {
                        $responseBodyEntry = new JsonEntry('response_body', (array) \json_decode($responseBodyContent, true, 512, JSON_THROW_ON_ERROR));
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

        return new Entries(
            $responseBodyEntry,
            new JsonEntry('response_headers', $response->getHeaders()),
            new IntegerEntry('response_status_code', $response->getStatusCode()),
            string_entry('response_protocol_version', $response->getProtocolVersion()),
            string_entry('response_reason_phrase', $response->getReasonPhrase()),
        );
    }
}

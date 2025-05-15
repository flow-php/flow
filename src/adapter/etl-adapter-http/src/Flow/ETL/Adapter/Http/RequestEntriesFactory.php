<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry\JsonEntry;
use Psr\Http\Message\RequestInterface;

final class RequestEntriesFactory
{
    /**
     * @param RequestInterface $request
     *
     * @throws \JsonException
     * @throws InvalidArgumentException
     *
     * @return Row\Entries
     */
    public function create(RequestInterface $request) : Entries
    {
        $requestType = 'html';

        if ($request->hasHeader('Content-Type')) {
            foreach ($request->getHeader('Content-Type') as $header) {
                if (\str_contains('application/json', $header)) {
                    $requestType = 'json';
                }
            }
        } else {
            foreach ($request->getHeader('Accept') as $header) {
                if (\str_contains('application/json', $header)) {
                    $requestType = 'json';
                }
            }
        }

        $requestBodyEntry = string_entry('request_body', null);
        $requestBody = $request->getBody();

        if ($requestBody->isReadable()) {
            if ($requestBody->isSeekable()) {
                $requestBody->seek(0);
            }

            $requestBodyContent = $requestBody->getContents();

            if ($requestBody->isSeekable()) {
                $requestBody->seek(0);
            }

            if (!empty($requestBodyContent)) {
                switch ($requestType) {
                    case 'json':
                        if (\class_exists(JsonEntry::class)) {
                            $requestBodyEntry = new JsonEntry('request_body', (array) \json_decode($requestBodyContent, true, 512, JSON_THROW_ON_ERROR));
                        } else {
                            $requestBodyEntry = string_entry('request_body', $requestBodyContent);
                        }

                        break;

                    default:
                        $requestBodyEntry = string_entry('request_body', $requestBodyContent);

                        break;
                }
            }
        }

        return new Entries(
            $requestBodyEntry,
            string_entry('request_uri', (string) $request->getUri()),
            new JsonEntry('request_headers', $request->getHeaders()),
            string_entry('request_protocol_version', $request->getProtocolVersion()),
            string_entry('request_method', $request->getMethod()),
        );
    }
}

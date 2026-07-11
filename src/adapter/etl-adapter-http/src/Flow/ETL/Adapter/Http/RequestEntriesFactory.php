<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\Types\Value\Json;
use Psr\Http\Message\RequestInterface;

use function class_exists;
use function Flow\ETL\DSL\string_entry;
use function Flow\Types\DSL\type_array;
use function json_decode;
use function str_contains;

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
    public function create(RequestInterface $request): Entries
    {
        $requestType = 'html';

        if ($request->hasHeader('Content-Type')) {
            foreach ($request->getHeader('Content-Type') as $header) {
                if (str_contains($header, 'application/json')) {
                    $requestType = 'json';
                }
            }
        } else {
            foreach ($request->getHeader('Accept') as $header) {
                if (str_contains($header, 'application/json')) {
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
                $requestBodyEntry = match ($requestType) {
                    'json' => class_exists(JsonEntry::class)
                        ? new JsonEntry(
                            'request_body',
                            Json::fromArray(type_array()->assert(json_decode(
                                $requestBodyContent,
                                true,
                                512,
                                JSON_THROW_ON_ERROR,
                            ))),
                        )
                        : string_entry('request_body', $requestBodyContent),
                    default => string_entry('request_body', $requestBodyContent),
                };
            }
        }

        return new Entries(
            $requestBodyEntry,
            string_entry('request_uri', (string) $request->getUri()),
            new JsonEntry('request_headers', Json::fromArray($request->getHeaders())),
            string_entry('request_protocol_version', $request->getProtocolVersion()),
            string_entry('request_method', $request->getMethod()),
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Psr\Http\Message\ResponseInterface;

use function Flow\ETL\DSL\html_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\Types\DSL\type_array;
use function json_decode;

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
    public function create(ResponseInterface $response): Entries
    {
        $responseBody = $response->getBody();

        if ($responseBody->isReadable()) {
            $responseType = ContentTypeDetector::detectFromResponse($response);

            if ($responseBody->isSeekable()) {
                $responseBody->seek(0);
            }

            $responseBodyContent = $responseBody->getContents();

            if ($responseBody->isSeekable()) {
                $responseBody->seek(0);
            }

            $responseBodyEntry = match ($responseType) {
                ResponseType::JSON => json_entry(
                    'response_body',
                    type_array()->assert(json_decode($responseBodyContent, true, 512, JSON_THROW_ON_ERROR)),
                ),
                ResponseType::XML => xml_entry('response_body', $responseBodyContent),
                ResponseType::HTML => class_exists('\Dom\HTMLDocument')
                    ? html_entry('response_body', $responseBodyContent)
                    : string_entry('response_body', $responseBodyContent),
                default => string_entry('response_body', $responseBodyContent),
            };
        } else {
            $responseBodyEntry = string_entry('response_body', null);
        }

        return new Entries(
            $responseBodyEntry,
            json_entry('response_headers', $response->getHeaders()),
            int_entry('response_status_code', $response->getStatusCode()),
            string_entry('response_protocol_version', $response->getProtocolVersion()),
            string_entry('response_reason_phrase', $response->getReasonPhrase()),
        );
    }
}

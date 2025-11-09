<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use function Flow\ETL\DSL\{html_entry, int_entry, json_entry, string_entry, xml_entry};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
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
        $responseBody = $response->getBody();

        if ($responseBody->isReadable()) {
            $responseType = ContentTypeDetector::detectFromHeaders($response->getHeader('Content-Type'));

            if ($responseBody->isSeekable()) {
                $responseBody->seek(0);
            }

            $responseBodyContent = $responseBody->getContents();

            if ($responseBody->isSeekable()) {
                $responseBody->seek(0);
            }

            $responseBodyEntry = match ($responseType) {
                'json' => json_entry('response_body', (array) \json_decode($responseBodyContent, true, 512, JSON_THROW_ON_ERROR)),
                'xml' => xml_entry('response_body', $responseBodyContent),
                default => string_entry('response_body', $responseBodyContent),
            };

            if (class_exists('\Dom\HTMLDocument') && 'html' === $responseType) {
                $responseBodyEntry = html_entry('response_body', $responseBodyContent);
            }
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

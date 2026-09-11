<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use DOMDocument;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\TypedRowValues;
use Flow\Types\Type\Logical\XML\XMLConverter;
use JsonException;
use Psr\Http\Message\MessageInterface;
use Psr\Http\Message\ResponseInterface;

use function Flow\Types\DSL\type_array;
use function json_decode;
use function mb_substr;
use function sprintf;

/**
 * A row carries the body as raw text, so response_body holds one type whatever the content type is.
 * Pagination, which needs to navigate into a structured body, calls structuredBody() instead.
 *
 * @implements Encoder<HttpExchange>
 */
final class HttpEncoder implements Encoder
{
    /**
     * @param list<HttpExchange> $batch
     *
     * @throws RuntimeException
     *
     * @return list<RawRowValues>
     */
    public function decode(array $batch): array
    {
        $rows = [];

        foreach ($batch as $exchange) {
            $response = $exchange->response;
            $request = $exchange->request;

            $rows[] = new RawRowValues([
                'response_body' => $this->body($response),
                'response_headers' => $response->getHeaders(),
                'response_status_code' => $response->getStatusCode(),
                'response_protocol_version' => $response->getProtocolVersion(),
                'response_reason_phrase' => $response->getReasonPhrase(),
                'request_body' => $this->body($request),
                'request_uri' => (string) $request->getUri(),
                'request_headers' => $request->getHeaders(),
                'request_protocol_version' => $request->getProtocolVersion(),
                'request_method' => $request->getMethod(),
            ]);
        }

        return $rows;
    }

    /**
     * @param list<TypedRowValues> $batch
     */
    public function encode(array $batch): array
    {
        throw new RuntimeException(
            'HTTP adapter is read-only, encoding Rows back to an HTTP exchange is not supported.',
        );
    }

    /**
     * @return array<mixed>
     */
    public function structuredBody(ResponseInterface $response): array
    {
        $content = $this->content($response);

        if ($content === null) {
            return [];
        }

        return match (ContentTypeDetector::detectFromResponse($response)) {
            ResponseType::JSON => $this->decodeJson($content, $response->getStatusCode()),
            ResponseType::XML => $this->decodeXml($content),
            default => [],
        };
    }

    private function body(MessageInterface $message): ?string
    {
        return $this->content($message);
    }

    private function content(MessageInterface $message): ?string
    {
        $body = $message->getBody();

        if (!$body->isReadable()) {
            return null;
        }

        if ($body->isSeekable()) {
            $body->seek(0);
        }

        $content = $body->getContents();

        if ($body->isSeekable()) {
            $body->seek(0);
        }

        return $content === '' ? null : $content;
    }

    /**
     * @return array<mixed>
     */
    private function decodeJson(string $content, ?int $statusCode): array
    {
        try {
            return type_array()->assert(json_decode($content, true, 512, JSON_THROW_ON_ERROR));
        } catch (JsonException $e) {
            throw new RuntimeException(
                sprintf(
                    'Failed to decode JSON body%s: %s. Body snippet: "%s"',
                    $statusCode === null ? '' : sprintf(' (status %d)', $statusCode),
                    $e->getMessage(),
                    mb_substr($content, 0, 256),
                ),
                previous: $e,
            );
        }
    }

    /**
     * @return array<mixed>
     */
    private function decodeXml(string $content): array
    {
        $document = new DOMDocument();

        if (!@$document->loadXML($content)) {
            throw new RuntimeException(sprintf('Failed to decode XML body: not valid XML. Body snippet: "%s"', mb_substr(
                $content,
                0,
                256,
            )));
        }

        return (new XMLConverter())->toArray($document);
    }
}

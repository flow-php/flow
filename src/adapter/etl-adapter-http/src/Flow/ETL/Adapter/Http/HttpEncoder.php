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
use Psr\Http\Message\RequestInterface;

use function Flow\Types\DSL\type_array;
use function json_decode;
use function mb_substr;
use function sprintf;

/**
 * Decodes an HTTP exchange into raw row values. The response/request body is decoded once, content-type aware,
 * into a navigable nested map (JSON via json_decode, XML via {@see XMLConverter}); the extractor's Hydrator types
 * the Row from it and pagination reads the same map.
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
                'response_body' => $this->body(
                    $response,
                    ContentTypeDetector::detectFromResponse($response),
                    $response->getStatusCode(),
                ),
                'response_headers' => $response->getHeaders(),
                'response_status_code' => $response->getStatusCode(),
                'response_protocol_version' => $response->getProtocolVersion(),
                'response_reason_phrase' => $response->getReasonPhrase(),
                'request_body' => $this->body($request, $this->requestType($request), null),
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

    private function body(MessageInterface $message, ResponseType $type, ?int $statusCode): array|string|null
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

        if ($content === '') {
            return null;
        }

        return match ($type) {
            ResponseType::JSON => $this->decodeJson($content, $statusCode),
            ResponseType::XML => $this->decodeXml($content),
            default => $content,
        };
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

    private function requestType(RequestInterface $request): ResponseType
    {
        foreach ($request->getHeader('Content-Type') as $header) {
            $type = ContentTypeDetector::detectFromHeader($header);

            if ($type !== null) {
                return $type;
            }
        }

        return ResponseType::TEXT;
    }
}

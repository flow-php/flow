<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http;

use Flow\Bridge\Monolog\Http\Sanitization\Sanitizer;
use JsonException;
use Monolog\LogRecord;
use Monolog\Processor\ProcessorInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

use function array_filter;
use function array_is_list;
use function array_keys;
use function count;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function in_array;
use function is_array;
use function is_string;
use function json_decode;
use function json_encode;
use function strtolower;
use function substr;

final readonly class PSR7Processor implements ProcessorInterface
{
    public function __construct(
        private Config $config = new Config(),
    ) {}

    public function __invoke(LogRecord $record): LogRecord
    {
        $context = $record->context;

        foreach (array_keys($context) as $key) {
            if ($context[$key] instanceof RequestInterface) {
                $context[$key] = $this->normalizeRequest($context[$key]);

                if (empty($context[$key])) {
                    unset($context[$key]);
                }

                continue;
            }

            if ($context[$key] instanceof ResponseInterface) {
                $context[$key] = $this->normalizeResponse($context[$key]);

                if (empty($context[$key])) {
                    unset($context[$key]);
                }
            }
        }

        return $record->with(context: $context);
    }

    private function isJson(string $body): bool
    {
        try {
            json_decode($body, false, 512, JSON_THROW_ON_ERROR);

            return true;
        } catch (JsonException) {
            return false;
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function normalizeRequest(RequestInterface $request): array
    {
        $requestData = [];

        if ($this->config->request->includeMethod()) {
            $requestData['method'] = $request->getMethod();
        }

        if ($this->config->request->includeUri()) {
            $requestData['uri'] = (string) $request->getUri();
        }

        if ($this->config->request->includeBody()) {
            $body = $request->getBody()->getContents();
            $request->getBody()->rewind();

            if ($this->isJson($body)) {
                $decodedBody = type_array()->assert(json_decode($body, true, 512, JSON_THROW_ON_ERROR));

                if (!array_is_list($decodedBody)) {
                    $sanitizedBody = $this->recursiveSanitize(
                        type_map(type_string(), type_mixed())->assert($decodedBody),
                        $this->config->request->sanitizers(),
                    );
                } else {
                    $sanitizedBody = [];
                }

                $requestData['body'] = substr(
                    json_encode($sanitizedBody, JSON_THROW_ON_ERROR),
                    0,
                    $this->config->request->bodySizeLimit(),
                );
            } else {
                $requestData['body'] = substr(
                    $request->getBody()->getContents(),
                    0,
                    $this->config->request->bodySizeLimit(),
                );
            }

            if ($requestData['body'] === '') {
                unset($requestData['body']);
            }
        }

        if ($this->config->request->includeHeaders()) {
            $requestData['headers'] = array_filter(
                $request->getHeaders(),
                fn(int|string $header) => in_array(
                    strtolower((string) $header),
                    $this->config->request->includeHeaders(),
                    true,
                ),
                ARRAY_FILTER_USE_KEY,
            );
        }

        return $this->recursiveSanitize($requestData, $this->config->request->sanitizers());
    }

    /**
     * @return array<string, mixed>
     */
    private function normalizeResponse(ResponseInterface $response): array
    {
        $responseData = [];

        if (in_array($response->getStatusCode(), $this->config->response->excludeStatusCodes(), true)) {
            return $responseData;
        }

        if ($this->config->response->includeStatus()) {
            $responseData['status'] = $response->getStatusCode();
        }

        if ($this->config->response->includeReasonPhrase()) {
            $responseData['reason_phrase'] = $response->getReasonPhrase();
        }

        if ($this->config->response->includeBody()) {
            $body = $response->getBody()->getContents();
            $response->getBody()->rewind();

            if ($this->isJson($body)) {
                $decodedBody = type_array()->assert(json_decode($body, true, 512, JSON_THROW_ON_ERROR));

                if (!array_is_list($decodedBody)) {
                    $sanitizedBody = $this->recursiveSanitize(
                        type_map(type_string(), type_mixed())->assert($decodedBody),
                        $this->config->response->sanitizers(),
                    );
                } else {
                    $sanitizedBody = [];
                }

                $responseData['body'] = substr(
                    json_encode($sanitizedBody, JSON_THROW_ON_ERROR),
                    0,
                    $this->config->response->bodySizeLimit(),
                );
            } else {
                $responseData['body'] = substr(
                    $response->getBody()->getContents(),
                    0,
                    $this->config->response->bodySizeLimit(),
                );
            }

            if ($responseData['body'] === '') {
                unset($responseData['body']);
            }
        }

        if ($this->config->response->includeHeaders()) {
            $responseData['headers'] = array_filter(
                $response->getHeaders(),
                fn(int|string $header) => in_array(
                    strtolower((string) $header),
                    $this->config->response->includeHeaders(),
                    true,
                ),
                ARRAY_FILTER_USE_KEY,
            );
        }

        return $this->recursiveSanitize($responseData, $this->config->response->sanitizers());
    }

    /**
     * Recursively sanitize an array by masking sensitive fields.
     *
     * @param array<string, mixed> $data
     * @param array<string, Sanitizer> $sanitizers
     *
     * @return array<string, mixed>
     */
    private function recursiveSanitize(array $data, array $sanitizers): array
    {
        if (!count($sanitizers)) {
            return $data;
        }

        foreach (array_keys($data) as $key) {
            if (is_array($data[$key]) && !array_is_list($data[$key])) {
                $data[$key] = $this->recursiveSanitize(
                    type_map(type_string(), type_mixed())->assert($data[$key]),
                    $sanitizers,
                );

                continue;
            }

            if (is_string($data[$key]) && isset($sanitizers[$key])) {
                $data[$key] = $sanitizers[$key]->sanitize($data[$key]);
            }
        }

        return $data;
    }
}

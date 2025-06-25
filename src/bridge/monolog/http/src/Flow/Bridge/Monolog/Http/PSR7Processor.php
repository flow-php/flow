<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http;

use function Flow\Types\DSL\type_array;
use Flow\Bridge\Monolog\Http\Sanitization\Sanitizer;
use Monolog\LogRecord;
use Monolog\Processor\ProcessorInterface;
use Psr\Http\Message\{RequestInterface, ResponseInterface};

final readonly class PSR7Processor implements ProcessorInterface
{
    public function __construct(private Config $config = new Config())
    {
    }

    /**
     * @param array<string, mixed>|LogRecord $record
     *
     * @return array<string, mixed>|LogRecord
     */
    public function __invoke(LogRecord|array $record) : LogRecord|array
    {
        $context = \is_array($record) ? $record['context'] : $record->context;
        $context = type_array()->assert($context);

        foreach ($context as $key => $val) {
            if ($val instanceof RequestInterface) {
                $context[$key] = $this->normalizeRequest($val);

                if (empty($context[$key])) {
                    unset($context[$key]);
                }
            }

            if ($val instanceof ResponseInterface) {
                $context[$key] = $this->normalizeResponse($val);

                if (empty($context[$key])) {
                    unset($context[$key]);
                }
            }
        }

        if (\is_array($record)) {
            $record['context'] = $context;

            return $record;
        }

        return $record->with(context: $context);
    }

    private function isJson(string $body) : bool
    {
        try {
            json_decode($body, false, 512, JSON_THROW_ON_ERROR);

            return true;
        } catch (\JsonException) {
            return false;
        }
    }

    /**
     * @return array<string, mixed>
     */
    private function normalizeRequest(RequestInterface $request) : array
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
                $decodedBody = \json_decode($body, true, 512, JSON_THROW_ON_ERROR);
                $decodedBody = type_array()->assert($decodedBody);
                /** @var array<string, mixed> $sanitizedBody */
                $sanitizedBody = \array_is_list($decodedBody) ? [] : $decodedBody;
                $body = $this->recursiveSanitize($sanitizedBody, $this->config->request->sanitizers());

                $requestData['body'] = \substr(\json_encode($body, JSON_THROW_ON_ERROR), 0, $this->config->request->bodySizeLimit());
            } else {
                $requestData['body'] = \substr($request->getBody()->getContents(), 0, $this->config->request->bodySizeLimit());
            }

            if ($requestData['body'] === '') {
                unset($requestData['body']);
            }
        }

        if ($this->config->request->includeHeaders()) {
            $requestData['headers'] = \array_filter(
                $request->getHeaders(),
                fn (string $header) => \in_array(\strtolower($header), $this->config->request->includeHeaders(), true),
                ARRAY_FILTER_USE_KEY
            );
        }

        return $this->recursiveSanitize($requestData, $this->config->request->sanitizers());
    }

    /**
     * @return array<string, mixed>
     */
    private function normalizeResponse(ResponseInterface $response) : array
    {
        $responseData = [];

        if (\in_array($response->getStatusCode(), $this->config->response->excludeStatusCodes(), true)) {
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
                $decodedBody = \json_decode($body, true, 512, JSON_THROW_ON_ERROR);
                $decodedBody = type_array()->assert($decodedBody);
                /** @var array<string, mixed> $sanitizedBody */
                $sanitizedBody = \array_is_list($decodedBody) ? [] : $decodedBody;
                $body = $this->recursiveSanitize($sanitizedBody, $this->config->response->sanitizers());

                $responseData['body'] = \substr(\json_encode($body, JSON_THROW_ON_ERROR), 0, $this->config->response->bodySizeLimit());
            } else {
                $responseData['body'] = \substr($response->getBody()->getContents(), 0, $this->config->response->bodySizeLimit());
            }

            if ($responseData['body'] === '') {
                unset($responseData['body']);
            }
        }

        if ($this->config->response->includeHeaders()) {
            $responseData['headers'] = \array_filter(
                $response->getHeaders(),
                fn (string $header) => \in_array(\strtolower($header), $this->config->response->includeHeaders(), true),
                ARRAY_FILTER_USE_KEY
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
    private function recursiveSanitize(array $data, array $sanitizers) : array
    {
        if (!\count($sanitizers)) {
            return $data;
        }

        foreach ($data as $key => $value) {
            if (\is_array($value)) {
                /** @phpstan-var array<string, mixed> $value */
                $data[$key] = $this->recursiveSanitize($value, $sanitizers);

                continue;
            }

            if (\is_string($value) && isset($sanitizers[$key])) {
                $data[$key] = $sanitizers[$key]->sanitize($value);
            }
        }

        return $data;
    }
}

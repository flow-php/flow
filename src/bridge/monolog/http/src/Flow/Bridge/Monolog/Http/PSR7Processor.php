<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http;

use Monolog\LogRecord;
use Monolog\Processor\ProcessorInterface;
use Psr\Http\Message\{RequestInterface, ResponseInterface};

final class PSR7Processor implements ProcessorInterface
{
    public function __construct(private readonly Config $config = new Config())
    {
    }

    public function __invoke(LogRecord|array $record) : LogRecord|array
    {
        $context = \is_array($record) ? $record['context'] : $record->context;

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
            $requestData['body'] = \substr($request->getBody()->getContents(), 0, $this->config->request->bodySizeLimit());
            $request->getBody()->rewind();

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

        return $requestData;
    }

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
            $responseData['body'] = \substr($response->getBody()->getContents(), 0, $this->config->response->bodySizeLimit());
            $response->getBody()->rewind();

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

        return $responseData;
    }
}

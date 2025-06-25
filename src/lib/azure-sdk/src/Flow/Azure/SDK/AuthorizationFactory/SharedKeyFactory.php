<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\AuthorizationFactory;

use Flow\Azure\SDK\AuthorizationFactory;
use Psr\Http\Message\RequestInterface;

final readonly class SharedKeyFactory implements AuthorizationFactory
{
    public function __construct(
        #[\SensitiveParameter]
        private string $account,
        #[\SensitiveParameter]
        private string $accountKey,
    ) {
    }

    public function for(RequestInterface $request) : string
    {
        $signature = $this->computeSignature(
            $this->normalizeHeaders($request),
            (string) $request->getUri(),
            $this->parseQueryPart($request->getUri()->getQuery()),
            $request->getMethod()
        );

        return 'SharedKey ' . $this->account . ':' . base64_encode(
            hash_hmac('sha256', $signature, (string) base64_decode($this->accountKey, true), true)
        );
    }

    /**
     * @param array<string, mixed> $headers
     *
     * @return array<int, string>
     */
    private function computeCanonicalizedHeaders(array $headers) : array
    {
        $canonicalizedHeaders = [];
        $normalizedHeaders = [];

        foreach ($headers as $header => $value) {
            $header = \strtolower($header);

            if (\str_starts_with($header, 'x-ms-')) {
                if (\is_string($value)) {
                    $stringValue = $value;
                } elseif (\is_int($value) || \is_float($value)) {
                    $stringValue = (string) $value;
                } elseif (\is_bool($value)) {
                    $stringValue = $value ? '1' : '0';
                } else {
                    $stringValue = '';
                }

                $stringValue = \str_replace("\r\n", ' ', $stringValue);
                $stringValue = \ltrim($stringValue);
                $header = \rtrim($header);

                $normalizedHeaders[$header] = $stringValue;
            }
        }

        \ksort($normalizedHeaders);

        foreach ($normalizedHeaders as $key => $value) {
            $canonicalizedHeaders[] = $key . ':' . $value;
        }

        return $canonicalizedHeaders;
    }

    /**
     * @param array<string, mixed> $queryParams
     */
    private function computeCanonicalizedResource(string $url, array $queryParams) : string
    {
        $queryParams = array_change_key_case($queryParams);

        $canonicalizedResource = '/' . $this->account;

        $canonicalizedResource .= parse_url($url, PHP_URL_PATH);

        if (\count($queryParams) > 0) {
            \ksort($queryParams);
        }

        foreach ($queryParams as $key => $value) {
            if (\is_string($value)) {
                $stringValue = $value;
            } elseif (\is_int($value) || \is_float($value)) {
                $stringValue = (string) $value;
            } elseif (\is_bool($value)) {
                $stringValue = $value ? '1' : '0';
            } else {
                $stringValue = '';
            }

            $canonicalizedResource .= "\n" . $key . ':' . $stringValue;
        }

        return $canonicalizedResource;
    }

    /**
     * @param array<string, mixed> $headers
     * @param array<string, mixed> $queryParams
     */
    private function computeSignature(array $headers, string $url, array $queryParams, string $httpMethod) : string
    {
        $canonicalizedHeaders = $this->computeCanonicalizedHeaders($headers);
        $canonicalizedResource = $this->computeCanonicalizedResource($url, $queryParams);

        $stringToSign = [];
        $stringToSign[] = \strtoupper($httpMethod);

        $includedHeaders = ['content-encoding', 'content-language', 'content-length', 'content-md5', 'content-type', 'date', 'if-modified-since', 'if-match', 'if-none-match', 'if-unmodified-since', 'range'];

        $lowercaseHeaders = array_change_key_case($headers);

        foreach ($includedHeaders as $header) {
            $stringToSign[] = \array_key_exists($header, $lowercaseHeaders) ? $lowercaseHeaders[$header] : null;
        }

        if (count($canonicalizedHeaders) > 0) {
            $stringToSign[] = \implode("\n", $canonicalizedHeaders);
        }

        $stringToSign[] = $canonicalizedResource;

        return \implode("\n", $stringToSign);
    }

    /**
     * @return array<string, string>
     */
    private function normalizeHeaders(RequestInterface $request) : array
    {
        $headers = [];

        foreach ($request->getHeaders() as $key => $value) {
            if (is_array($value) && count($value) == 1) {
                $headers[strtolower($key)] = $value[0];
            } elseif (is_array($value)) {
                $headers[strtolower($key)] = implode(',', $value);
            } else {
                $headers[strtolower($key)] = (string) $value;
            }
        }

        return $headers;
    }

    /**
     * @return array<string, string>
     */
    private function parseQueryPart(string $queryPart, bool $urlEncoding = true) : array
    {
        /** @var array<string, string> $result */
        $result = [];

        if ($queryPart === '') {
            return $result;
        }

        if ($urlEncoding === true) {
            $decoder = static fn (string $value) : string => rawurldecode(str_replace('+', ' ', $value));
        } else {
            $decoder = static fn (string $str) : string => $str;
        }

        /** @var array<string, array<string>|string> $temporaryResult */
        $temporaryResult = [];

        foreach (explode('&', $queryPart) as $kvp) {
            $parts = explode('=', $kvp, 2);
            $key = $decoder($parts[0]);
            $value = isset($parts[1]) ? $decoder($parts[1]) : '';

            if (!array_key_exists($key, $temporaryResult)) {
                $temporaryResult[$key] = $value;
            } else {
                if (!is_array($temporaryResult[$key])) {
                    $temporaryResult[$key] = [$temporaryResult[$key]];
                }
                $temporaryResult[$key][] = $value;
            }
        }

        // Convert arrays to comma-separated strings to match return type
        foreach ($temporaryResult as $key => $value) {
            if (is_array($value)) {
                $result[$key] = implode(',', $value);
            } else {
                $result[$key] = $value;
            }
        }

        return $result;
    }
}

<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\AuthorizationFactory;

use Flow\Azure\SDK\AuthorizationFactory;
use Psr\Http\Message\RequestInterface;

final class SharedKeyFactory implements AuthorizationFactory
{
    public function __construct(
        #[\SensitiveParameter]
        private readonly string $account,
        #[\SensitiveParameter]
        private readonly string $accountKey,
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

    private function computeCanonicalizedHeaders(array $headers) : array
    {
        $canonicalizedHeaders = [];
        $normalizedHeaders = [];

        foreach ($headers as $header => $value) {
            $header = \strtolower($header);

            if (\str_starts_with($header, 'x-ms-')) {
                $value = \str_replace("\r\n", ' ', $value);

                $value = \ltrim($value);
                $header = \rtrim($header);

                $normalizedHeaders[$header] = $value;
            }
        }

        \ksort($normalizedHeaders);

        foreach ($normalizedHeaders as $key => $value) {
            $canonicalizedHeaders[] = $key . ':' . $value;
        }

        return $canonicalizedHeaders;
    }

    private function computeCanonicalizedResource(string $url, array $queryParams) : string
    {
        $queryParams = array_change_key_case($queryParams);

        $canonicalizedResource = '/' . $this->account;

        $canonicalizedResource .= parse_url($url, PHP_URL_PATH);

        if (\count($queryParams) > 0) {
            \ksort($queryParams);
        }

        foreach ($queryParams as $key => $value) {
            $canonicalizedResource .= "\n" . $key . ':' . $value;
        }

        return $canonicalizedResource;
    }

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

    private function normalizeHeaders(RequestInterface $request) : array
    {
        $headers = [];

        foreach ($request->getHeaders() as $key => $value) {
            if (is_array($value) && count($value) == 1) {
                $headers[strtolower($key)] = $value[0];
            } else {
                $headers[strtolower($key)] = $value;
            }
        }

        return $headers;
    }

    private function parseQueryPart(string $queryPart, bool $urlEncoding = true) : array
    {
        $result = [];

        if ($queryPart === '') {
            return $result;
        }

        if ($urlEncoding === true) {
            $decoder = static fn (string $value) : string => rawurldecode(str_replace('+', ' ', $value));
        } else {
            $decoder = static fn (string $str) : string => $str;
        }

        foreach (explode('&', $queryPart) as $kvp) {
            $parts = explode('=', $kvp, 2);
            $key = $decoder($parts[0]);
            $value = isset($parts[1]) ? $decoder($parts[1]) : null;

            if (!array_key_exists($key, $result)) {
                $result[$key] = $value;
            } else {
                if (!is_array($result[$key])) {
                    $result[$key] = [$result[$key]];
                }
                $result[$key][] = $value;
            }
        }

        return $result;
    }
}

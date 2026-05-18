<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\AuthorizationFactory;

use Flow\Azure\SDK\AuthorizationFactory;
use Psr\Http\Message\RequestInterface;
use SensitiveParameter;

use function count;
use function implode;
use function ksort;
use function ltrim;
use function rtrim;
use function str_replace;
use function str_starts_with;
use function strtolower;
use function strtoupper;

final readonly class SharedKeyFactory implements AuthorizationFactory
{
    public function __construct(
        #[SensitiveParameter]
        private string $account,
        #[SensitiveParameter]
        private string $accountKey,
    ) {}

    public function for(RequestInterface $request): string
    {
        $signature = $this->computeSignature(
            $this->normalizeHeaders($request),
            (string) $request->getUri(),
            $this->parseQueryPart($request->getUri()->getQuery()),
            $request->getMethod(),
        );

        return (
            'SharedKey '
            . $this->account
            . ':'
            . base64_encode(hash_hmac('sha256', $signature, (string) base64_decode($this->accountKey, true), true))
        );
    }

    /**
     * @param array<string, string> $headers
     *
     * @return array<int, string>
     */
    private function computeCanonicalizedHeaders(array $headers): array
    {
        $canonicalizedHeaders = [];
        $normalizedHeaders = [];

        foreach ($headers as $header => $value) {
            $header = strtolower($header);

            if (str_starts_with($header, 'x-ms-')) {
                $stringValue = ltrim(str_replace("\r\n", ' ', $value));
                $header = rtrim($header);

                $normalizedHeaders[$header] = $stringValue;
            }
        }

        ksort($normalizedHeaders);

        foreach ($normalizedHeaders as $key => $value) {
            $canonicalizedHeaders[] = $key . ':' . $value;
        }

        return $canonicalizedHeaders;
    }

    /**
     * @param array<string, string> $queryParams
     */
    private function computeCanonicalizedResource(string $url, array $queryParams): string
    {
        $queryParams = array_change_key_case($queryParams);

        $canonicalizedResource = '/' . $this->account;

        $canonicalizedResource .= (string) parse_url($url, PHP_URL_PATH);

        if (count($queryParams) > 0) {
            ksort($queryParams);
        }

        foreach ($queryParams as $key => $value) {
            $canonicalizedResource .= "\n" . $key . ':' . $value;
        }

        return $canonicalizedResource;
    }

    /**
     * @param array<string, string> $headers
     * @param array<string, string> $queryParams
     */
    private function computeSignature(array $headers, string $url, array $queryParams, string $httpMethod): string
    {
        $canonicalizedHeaders = $this->computeCanonicalizedHeaders($headers);
        $canonicalizedResource = $this->computeCanonicalizedResource($url, $queryParams);

        $stringToSign = [];
        $stringToSign[] = strtoupper($httpMethod);

        $includedHeaders = [
            'content-encoding',
            'content-language',
            'content-length',
            'content-md5',
            'content-type',
            'date',
            'if-modified-since',
            'if-match',
            'if-none-match',
            'if-unmodified-since',
            'range',
        ];

        $lowercaseHeaders = array_change_key_case($headers);

        foreach ($includedHeaders as $header) {
            $stringToSign[] = $lowercaseHeaders[$header] ?? '';
        }

        if (count($canonicalizedHeaders) > 0) {
            $stringToSign[] = implode("\n", $canonicalizedHeaders);
        }

        $stringToSign[] = $canonicalizedResource;

        return implode("\n", $stringToSign);
    }

    /**
     * @return array<string, string>
     */
    private function normalizeHeaders(RequestInterface $request): array
    {
        $headers = [];

        foreach ($request->getHeaders() as $key => $value) {
            $headers[strtolower((string) $key)] = count($value) === 1 ? $value[0] : implode(',', $value);
        }

        return $headers;
    }

    /**
     * @return array<string, string>
     */
    private function parseQueryPart(string $queryPart, bool $urlEncoding = true): array
    {
        $result = [];

        if ($queryPart === '') {
            return $result;
        }

        if ($urlEncoding === true) {
            $decoder = static fn(string $value): string => rawurldecode(str_replace('+', ' ', $value));
        } else {
            $decoder = static fn(string $str): string => $str;
        }

        /** @var array<string, array<int, string>|string> $temporaryResult */
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

        foreach ($temporaryResult as $key => $value) {
            $result[$key] = is_array($value) ? implode(',', $value) : $value;
        }

        return $result;
    }
}

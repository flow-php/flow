<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Mother;

use Flow\Azure\SDK\BlobServiceInterface;
use Psr\Log\NullLogger;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Psr18Client;
use Symfony\Component\HttpClient\Response\MockResponse;

use function array_key_exists;
use function base64_encode;
use function Flow\Azure\SDK\DSL\azure_blob_service;
use function Flow\Azure\SDK\DSL\azure_blob_service_config;
use function Flow\Azure\SDK\DSL\azure_http_factory;
use function Flow\Azure\SDK\DSL\azure_shared_key_authorization_factory;
use function Flow\Azure\SDK\DSL\azure_url_factory;
use function parse_str;
use function parse_url;
use function strlen;
use function substr;
use function trim;

use const PHP_URL_QUERY;

final class BlobServiceMother
{
    /**
     * @param array<string, array{int, array<string, string>}> $answers "Operation" answers every such request, "Operation#n" its n-th request only
     * @param list<string> $calls receives "Operation" per request, "PutBlockFromUrl bytes=<first>-<last>" for a ranged block
     * @param list<array<string, string>> $putBlockListHeaders receives the lowercased headers of every PutBlockList request
     */
    public static function answering(
        array $answers,
        array &$calls,
        array &$putBlockListHeaders = [],
    ): BlobServiceInterface {
        /** @var array<string, int> $counts */
        $counts = [];

        $client = new Psr18Client(new MockHttpClient(
            /**
             * @param array{normalized_headers: array<string, list<string>>, ...} $options
             */
            static function (string $method, string $url, array $options) use (
                $answers,
                &$calls,
                &$putBlockListHeaders,
                &$counts,
            ): MockResponse {
                $query = [];
                parse_str((string) parse_url($url, PHP_URL_QUERY), $query);
                /** @var array<string, string> $headers */
                $headers = [];

                foreach ($options['normalized_headers'] as $name => $values) {
                    $headers[$name] = trim(substr($values[0], strlen($name) + 1));
                }

                $copySource = array_key_exists('x-ms-copy-source', $headers);

                $name = match (true) {
                    $method === 'HEAD' => 'GetBlobProperties',
                    $method === 'DELETE' => 'DeleteBlob',
                    $method === 'PUT' && ($query['comp'] ?? null) === 'blocklist' => 'PutBlockList',
                    $method === 'PUT' && ($query['comp'] ?? null) === 'block' && $copySource => 'PutBlockFromUrl',
                    $method === 'PUT' && $copySource && ($headers['x-ms-requires-sync'] ?? null) === 'true'
                        => 'CopyBlob',
                    default => 'Unexpected ' . $method . ' ' . $url,
                };
                $counts[$name] = ($counts[$name] ?? 0) + 1;
                $calls[] = $name === 'PutBlockFromUrl' ? $name . ' ' . ($headers['x-ms-source-range'] ?? '') : $name;

                if ($name === 'PutBlockList') {
                    $putBlockListHeaders[] = $headers;
                }

                [$status, $responseHeaders] = $answers[$name . '#' . $counts[$name]] ?? $answers[$name] ?? [500, []];

                return new MockResponse('', ['http_code' => $status, 'response_headers' => $responseHeaders]);
            },
        ));

        return azure_blob_service(
            azure_blob_service_config('account', 'container'),
            azure_shared_key_authorization_factory('account', base64_encode('key')),
            $client,
            azure_http_factory($client, $client),
            azure_url_factory(),
            new NullLogger(),
        );
    }
}

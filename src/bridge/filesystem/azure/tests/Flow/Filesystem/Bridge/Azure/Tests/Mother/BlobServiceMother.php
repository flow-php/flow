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
use function htmlspecialchars;
use function implode;
use function parse_str;
use function parse_url;
use function strlen;
use function substr;
use function trim;

use const ENT_XML1;
use const PHP_URL_QUERY;

final class BlobServiceMother
{
    /**
     * @param array<string, array{int, array<string, string>, 2?: string}> $answers "Operation" answers every such request, "Operation#n" its n-th request only
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
                    $method === 'GET' && ($query['comp'] ?? null) === 'list' => 'ListBlobs',
                    $method === 'POST' && ($query['comp'] ?? null) === 'batch' => 'BlobBatch',
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

                $answer = $answers[$name . '#' . $counts[$name]] ?? $answers[$name] ?? [500, [], ''];

                return new MockResponse($answer[2] ?? '', [
                    'http_code' => $answer[0],
                    'response_headers' => $answer[1],
                ]);
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

    public static function batchAnswer(int ...$statuses): string
    {
        $parts = [];

        foreach ($statuses as $contentId => $status) {
            $errorCode = match ($status) {
                202 => '',
                403 => "x-ms-error-code: AuthorizationFailure\r\n",
                404 => "x-ms-error-code: BlobNotFound\r\n",
                default => "x-ms-error-code: InternalError\r\n",
            };
            $parts[] =
                "--batchresponse_test\r\nContent-Type: application/http\r\nContent-ID: {$contentId}\r\n\r\n"
                . "HTTP/1.1 {$status} Status\r\n{$errorCode}x-ms-version: 2024-08-04\r\n\r\n";
        }

        return implode('', $parts) . "--batchresponse_test--\r\n";
    }

    public static function listing(string ...$blobs): string
    {
        $entries = [];

        foreach ($blobs as $blob) {
            $entries[] =
                '<Blob><Name>'
                . htmlspecialchars($blob, ENT_XML1)
                . '</Name><Properties>'
                . '<Last-Modified>Wed, 18 Nov 2026 12:00:00 GMT</Last-Modified><Content-Length>1</Content-Length>'
                . '</Properties></Blob>';
        }

        return (
            '<?xml version="1.0" encoding="utf-8"?><EnumerationResults ContainerName="container"><Blobs>'
            . implode('', $entries)
            . '</Blobs></EnumerationResults>'
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Mother;

use AsyncAws\S3\S3Client;
use RuntimeException;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

use function parse_url;
use function preg_match;
use function str_contains;
use function stripos;
use function strlen;
use function substr;
use function trim;

use const PHP_URL_QUERY;

final class S3ClientMother
{
    public static function serving(string $content): S3Client
    {
        return new S3Client(
            ['region' => 'eu-west-1', 'accessKeyId' => 'k', 'accessKeySecret' => 's', 'endpoint' => 'http://s3.test'],
            null,
            new MockHttpClient(
                /**
                 * @param array{headers: list<string>, ...} $options
                 */
                static function (string $method, string $url, array $options) use ($content): MockResponse {
                    if ($method === 'HEAD') {
                        return new MockResponse('', [
                            'http_code' => 200,
                            'response_headers' => ['content-length' => (string) strlen($content)],
                        ]);
                    }

                    $range = '';
                    $matches = [];

                    foreach ($options['headers'] as $header) {
                        if (stripos($header, 'range:') === 0) {
                            $range = trim(substr($header, 6));
                        }
                    }

                    preg_match('/bytes=(\d+)-(\d*)/', $range, $matches);
                    $start = (int) $matches[1];

                    return new MockResponse(
                        substr($content, $start, $matches[2] === '' ? null : (int) $matches[2] - $start + 1),
                        ['http_code' => 206],
                    );
                },
            ),
        );
    }

    public static function shortRead(int $size, int $maxReads = 3): S3Client
    {
        $reads = 0;

        return new S3Client(
            ['region' => 'eu-west-1', 'accessKeyId' => 'k', 'accessKeySecret' => 's', 'endpoint' => 'http://s3.test'],
            null,
            new MockHttpClient(static function (string $method) use ($size, $maxReads, &$reads): MockResponse {
                if ($method === 'HEAD') {
                    return new MockResponse('', [
                        'http_code' => 200,
                        'response_headers' => ['content-length' => (string) $size],
                    ]);
                }

                if (++$reads > $maxReads) {
                    throw new RuntimeException(
                        "short-read double: more than {$maxReads} reads, the loop does not stop",
                    );
                }

                return new MockResponse('', ['http_code' => 206]);
            }),
        );
    }

    /**
     * @param list<string> $calls receives "METHOD query" of every request
     */
    public static function completingWith(int $status, array &$calls): S3Client
    {
        return new S3Client(
            ['region' => 'eu-west-1', 'accessKeyId' => 'k', 'accessKeySecret' => 's', 'endpoint' => 'http://s3.test'],
            null,
            new MockHttpClient(static function (string $method, string $url) use ($status, &$calls): MockResponse {
                $query = (string) parse_url($url, PHP_URL_QUERY);
                $calls[] = $method . ' ' . $query;

                if ($method === 'POST' && str_contains($query, 'uploads')) {
                    return new MockResponse(
                        '<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>',
                    );
                }

                if ($method === 'PUT') {
                    return new MockResponse('', ['response_headers' => ['etag' => '"etag-1"']]);
                }

                if ($method === 'POST') {
                    return new MockResponse('<Error><Code>Status' . $status . '</Code></Error>', [
                        'http_code' => $status,
                    ]);
                }

                return new MockResponse('', ['http_code' => 204]);
            }),
        );
    }
}

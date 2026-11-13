<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Mother;

use AsyncAws\S3\S3Client;
use RuntimeException;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;

use function array_key_exists;
use function in_array;
use function ltrim;
use function max;
use function parse_str;
use function parse_url;
use function preg_match;
use function rtrim;
use function str_contains;
use function str_repeat;
use function str_starts_with;
use function stripos;
use function strlen;
use function substr;
use function trim;

use const PHP_URL_PATH;
use const PHP_URL_QUERY;

final class S3ClientMother
{
    public const string ERROR_XML = '<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>We encountered an internal error.</Message><RequestId>r</RequestId></Error>';

    /**
     * @param list<string> $calls receives "Operation key range" per request, e.g. "UploadPartCopy dst.bin bytes=0-5242879"
     */
    public static function copying(int $sourceSize, array &$calls): S3Client
    {
        return self::copyingAnswering($sourceSize, '', new MockResponse(), $calls);
    }

    /**
     * @param string $operation "Operation" answers on every such request, "Operation#n" on its n-th request only
     * @param list<string> $calls receives "Operation key range" per request
     */
    public static function copyingAnswering(
        int $sourceSize,
        string $operation,
        MockResponse $answer,
        array &$calls,
    ): S3Client {
        /** @var array<string, int> $counts */
        $counts = [];

        return new S3Client(
            ['region' => 'eu-west-1', 'accessKeyId' => 'k', 'accessKeySecret' => 's', 'endpoint' => 'http://s3.test'],
            null,
            new MockHttpClient(
                /**
                 * @param array{normalized_headers: array<string, list<string>>, ...} $options
                 */
                static function (string $method, string $url, array $options) use (
                    $sourceSize,
                    $operation,
                    $answer,
                    &$calls,
                    &$counts,
                ): MockResponse {
                    $query = [];
                    parse_str((string) parse_url($url, PHP_URL_QUERY), $query);
                    $headers = $options['normalized_headers'];
                    $copySource = array_key_exists('x-amz-copy-source', $headers);
                    $range = '';

                    foreach (['x-amz-copy-source-range', 'range'] as $rangeHeader) {
                        if (array_key_exists($rangeHeader, $headers)) {
                            $range = trim(substr($headers[$rangeHeader][0], strlen($rangeHeader) + 1));
                        }
                    }

                    $name = match (true) {
                        $method === 'HEAD' => 'HeadObject',
                        $method === 'PUT' && $copySource && array_key_exists('uploadId', $query) => 'UploadPartCopy',
                        $method === 'PUT' && $copySource => 'CopyObject',
                        $method === 'PUT' => 'UploadPart',
                        $method === 'POST' && array_key_exists('uploads', $query) => 'CreateMultipartUpload',
                        $method === 'POST' => 'CompleteMultipartUpload',
                        $method === 'DELETE' && array_key_exists('uploadId', $query) => 'AbortMultipartUpload',
                        $method === 'DELETE' => 'DeleteObject',
                        default => 'GetObject',
                    };
                    $counts[$name] = ($counts[$name] ?? 0) + 1;
                    $calls[] = rtrim($name . ' ' . ltrim((string) parse_url($url, PHP_URL_PATH), '/') . ' ' . $range);

                    if ($operation === $name || $operation === $name . '#' . $counts[$name]) {
                        return $answer;
                    }

                    $matches = [];

                    return match ($name) {
                        'HeadObject' => new MockResponse('', [
                            'response_headers' => ['content-length' => (string) $sourceSize],
                        ]),
                        'UploadPartCopy' => new MockResponse('<CopyPartResult><ETag>"p"</ETag></CopyPartResult>'),
                        'CopyObject' => new MockResponse('<CopyObjectResult><ETag>"c"</ETag></CopyObjectResult>'),
                        'UploadPart' => new MockResponse('', ['response_headers' => ['etag' => '"u"']]),
                        'CreateMultipartUpload' => new MockResponse(
                            '<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>',
                        ),
                        'CompleteMultipartUpload' => new MockResponse(
                            '<CompleteMultipartUploadResult><ETag>"m-2"</ETag></CompleteMultipartUploadResult>',
                        ),
                        'AbortMultipartUpload', 'DeleteObject' => new MockResponse('', ['http_code' => 204]),
                        default => new MockResponse(
                            preg_match('/bytes=(\d+)-(\d+)/', $range, $matches) === 1
                                ? str_repeat('x', max(0, (int) $matches[2] - (int) $matches[1] + 1))
                                : str_repeat('x', max(0, $sourceSize)),
                            ['http_code' => $range === '' ? 200 : 206],
                        ),
                    };
                },
            ),
        );
    }

    /**
     * @param array<string, string> $headHeaders answered by HeadObject next to content-length
     * @param array<string, string> $uploadHeaders receives the object attribute headers of CreateMultipartUpload
     */
    public static function copyingObjectWith(int $sourceSize, array $headHeaders, array &$uploadHeaders): S3Client
    {
        return new S3Client(
            ['region' => 'eu-west-1', 'accessKeyId' => 'k', 'accessKeySecret' => 's', 'endpoint' => 'http://s3.test'],
            null,
            new MockHttpClient(
                /**
                 * @param array{normalized_headers: array<string, list<string>>, ...} $options
                 */
                static function (string $method, string $url, array $options) use (
                    $sourceSize,
                    $headHeaders,
                    &$uploadHeaders,
                ): MockResponse {
                    $query = [];
                    parse_str((string) parse_url($url, PHP_URL_QUERY), $query);

                    if ($method === 'HEAD') {
                        return new MockResponse('', [
                            'response_headers' => [...$headHeaders, 'content-length' => (string) $sourceSize],
                        ]);
                    }

                    if ($method === 'POST' && array_key_exists('uploads', $query)) {
                        foreach ($options['normalized_headers'] as $name => $values) {
                            if (
                                in_array(
                                    $name,
                                    [
                                        'content-type',
                                        'content-encoding',
                                        'cache-control',
                                        'content-disposition',
                                        'content-language',
                                    ],
                                    true,
                                )
                                || str_starts_with($name, 'x-amz-meta-')
                            ) {
                                $uploadHeaders[$name] = trim(substr($values[0], strlen($name) + 1));
                            }
                        }

                        return new MockResponse(
                            '<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>',
                        );
                    }

                    if ($method === 'PUT') {
                        return new MockResponse('<CopyPartResult><ETag>"p"</ETag></CopyPartResult>');
                    }

                    return new MockResponse(
                        '<CompleteMultipartUploadResult><ETag>"m-1"</ETag></CompleteMultipartUploadResult>',
                    );
                },
            ),
        );
    }

    /**
     * @param string $operation "CopyObject", "UploadPartCopy" or "CompleteMultipartUpload"
     * @param list<string> $calls
     */
    public static function copyingWithErrorXmlOn(int $sourceSize, string $operation, array &$calls): S3Client
    {
        return self::copyingAnswering($sourceSize, $operation, new MockResponse(self::ERROR_XML), $calls);
    }

    /**
     * @param string $operation "UploadPartCopy#2" or "CompleteMultipartUpload"
     * @param list<string> $calls
     */
    public static function copyingWithStatusOn(int $sourceSize, string $operation, int $status, array &$calls): S3Client
    {
        return self::copyingAnswering(
            $sourceSize,
            $operation,
            new MockResponse(self::ERROR_XML, ['http_code' => $status]),
            $calls,
        );
    }

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

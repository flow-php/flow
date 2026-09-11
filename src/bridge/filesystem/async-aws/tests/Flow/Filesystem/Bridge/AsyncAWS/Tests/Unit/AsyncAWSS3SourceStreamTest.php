<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3SourceStream;
use Flow\Filesystem\Bridge\AsyncAWS\Tests\Mother\S3ClientMother;
use Flow\Filesystem\Tests\Context\ReadLinesContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class AsyncAWSS3SourceStreamTest extends TestCase
{
    /**
     * @param non-empty-string $separator
     * @param null|int<1, max> $length
     * @param list<string> $expected
     */
    #[DataProviderExternal(ReadLinesContext::class, 'cases')]
    public function test_read_lines_conforms_to_the_source_stream_contract(
        string $content,
        string $separator,
        ?int $length,
        array $expected,
    ): void {
        static::assertSame(
            $expected,
            iterator_to_array((new AsyncAWSS3SourceStream(
                path('aws-s3://f.txt'),
                'bucket',
                S3ClientMother::serving($content),
            ))->readLines($separator, $length)),
        );
    }

    public function test_read_lines_stops_when_a_read_returns_no_bytes(): void
    {
        static::assertSame(
            [],
            iterator_to_array(
                (new AsyncAWSS3SourceStream(
                    path('aws-s3://f.txt'),
                    'bucket',
                    S3ClientMother::shortRead(5),
                ))->readLines(),
            ),
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\SFTP\SFTPSourceStream;
use Flow\Filesystem\Bridge\SFTP\Tests\Double\FixedContentSFTP;
use Flow\Filesystem\Tests\Context\ReadLinesContext;
use PHPUnit\Framework\Attributes\DataProviderExternal;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class SFTPSourceStreamTest extends FlowTestCase
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
        $stream = new SFTPSourceStream(path('sftp:///upload/lines.txt'), FixedContentSFTP::serving($content));

        static::assertSame($expected, iterator_to_array($stream->readLines($separator, $length), false));
    }

    public function test_read_lines_stops_when_a_read_returns_no_bytes(): void
    {
        $stream = new SFTPSourceStream(path('sftp:///upload/lines.txt'), FixedContentSFTP::shortRead(5));

        static::assertSame([], iterator_to_array($stream->readLines(), false));
    }
}

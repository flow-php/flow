<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\JSON\IO\Extractor\JsonLineResync;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\SourceStream;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class JsonLineResyncTest extends FlowTestCase
{
    public static function stream(string $content): SourceStream
    {
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://lines.jsonl'));
        $stream->append($content);
        $stream->close();

        return $filesystem->readFrom(path('memory://lines.jsonl'));
    }

    /**
     * @param int<0, max> $from
     */
    #[TestWith([0, 0])]
    #[TestWith([1, 8])]
    #[TestWith([7, 8])]
    #[TestWith([8, 8])]
    #[TestWith([9, 16])]
    #[TestWith([16, 16])]
    #[TestWith([17, null])]
    public function test_after_is_the_first_line_start_at_or_after_the_offset(int $from, ?int $expected): void
    {
        static::assertSame($expected, (new JsonLineResync())->after(
            self::stream("{\"a\":1}\n{\"b\":2}\n{\"c\"}"),
            $from,
        ));
    }

    public function test_an_empty_stream_has_no_line_start(): void
    {
        static::assertNull((new JsonLineResync())->after(self::stream(''), 0));
    }

    public function test_crlf_lines_resync_after_the_newline(): void
    {
        static::assertSame(9, (new JsonLineResync())->after(self::stream("{\"a\":1}\r\n{\"b\":2}\r\n"), 3));
    }

    public function test_neighbouring_windows_cut_at_after_share_every_boundary_and_cover_the_file(): void
    {
        $content = "{\"a\":1}\n{\"bb\":22}\n{\"ccc\":333}\n{\"d\":4}";
        $stream = self::stream($content);
        $resync = new JsonLineResync();
        $read = '';

        foreach ([[0, 5], [5, 5], [10, 5], [15, 5], [20, 5], [25, 5], [30, 5], [35, null]] as [$start, $length]) {
            $read .= (new FileSplit(path('memory://lines.jsonl'), $start, $length))
                ->window(
                    $stream,
                    $resync->after($stream, $start),
                    $length === null ? null : $resync->after($stream, $start + $length),
                )
                ->content();
        }

        static::assertSame($content, $read);
    }
}

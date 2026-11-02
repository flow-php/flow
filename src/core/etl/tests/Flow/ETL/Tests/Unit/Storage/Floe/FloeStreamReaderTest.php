<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage\Floe;

use Flow\ETL\Storage\Floe\FloeFiles;
use Flow\ETL\Storage\Floe\FloeStreamReader;
use Flow\ETL\Storage\Floe\Format;
use Flow\ETL\Tests\Context\FloeStreamReaderContext;
use Flow\ETL\Tests\Double\ClosingSpySourceStream;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Frame\FloeException;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use PHPUnit\Framework\TestCase;

use function array_column;
use function array_filter;
use function array_keys;
use function array_map;
use function array_merge;
use function ceil;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function implode;
use function iterator_to_array;
use function range;
use function str_repeat;
use function strlen;

final class FloeStreamReaderTest extends TestCase
{
    public function test_close_closes_the_source_stream(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://close.floe');

        $data = array_to_rows([['id' => 1]], schema(int_schema('id')));
        $writer = (new FloeFiles($filesystem, new AdaptiveBackend()))->create($path, $data->schema());
        $writer->write($data);
        $writer->close();

        $source = new ClosingSpySourceStream($filesystem->readFrom($path));

        (new FloeStreamReader($source, new NoopCodec(), 65_536, new AdaptiveBackend()))->close();

        static::assertSame(1, $source->closeCount);
    }

    public function test_a_row_frame_is_an_unknown_frame_type(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://row-frame.floe');
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $path,
            schema(int_schema('id')),
            Format::frame(0x02, "\x01"),
            1,
        );

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('Floe found unknown frame type 0x02');

        iterator_to_array(
            (new FloeFiles($filesystem, new AdaptiveBackend()))
                ->read($path)
                ->rows(),
            false,
        );
    }

    public function test_offset_skips_a_whole_batch_frame_and_slices_the_next(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://offset.floe');
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $path,
            schema(int_schema('id')),
            Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(RowsMother::ids(1, 3), new NoopCodec()))
                . Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(
                    RowsMother::ids(4, 6),
                    new NoopCodec(),
                )),
            6,
        );

        static::assertSame(
            [[['id' => 5], ['id' => 6]]],
            array_map(
                static fn(Rows $batch) => $batch->toArray(),
                iterator_to_array(
                    (new FloeFiles($filesystem, new AdaptiveBackend()))
                        ->read($path)
                        ->rows(offset: 4),
                    false,
                ),
            ),
        );
    }

    public function test_limit_slices_a_batch_frame(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://limit.floe');
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $path,
            schema(int_schema('id')),
            Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(RowsMother::ids(1, 5), new NoopCodec())),
            5,
        );

        static::assertSame(
            [[['id' => 1], ['id' => 2]]],
            array_map(
                static fn(Rows $batch) => $batch->toArray(),
                iterator_to_array(
                    (new FloeFiles($filesystem, new AdaptiveBackend()))
                        ->read($path)
                        ->rows(limit: 2),
                    false,
                ),
            ),
        );
    }

    public function test_batch_size_is_an_upper_bound(): void
    {
        $filesystem = memory_filesystem();
        $one = path('memory://one-frame.floe');
        $two = path('memory://two-frames.floe');
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $one,
            schema(int_schema('id')),
            Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(RowsMother::ids(1, 5), new NoopCodec())),
            5,
        );
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $two,
            schema(int_schema('id')),
            Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(RowsMother::ids(1, 2), new NoopCodec()))
                . Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(
                    RowsMother::ids(3, 4),
                    new NoopCodec(),
                )),
            4,
        );

        static::assertSame(
            [2, 2, 1],
            array_map(
                static fn(Rows $batch) => $batch->count(),
                iterator_to_array(
                    (new FloeFiles($filesystem, new AdaptiveBackend()))
                        ->read($one)
                        ->rows(2),
                    false,
                ),
            ),
        );
        static::assertSame(
            [2, 2],
            array_map(
                static fn(Rows $batch) => $batch->count(),
                iterator_to_array(
                    (new FloeFiles($filesystem, new AdaptiveBackend()))
                        ->read($two)
                        ->rows(10),
                    false,
                ),
            ),
        );
    }

    public function test_chunked_frames_yield_unique_keys(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://keys.floe');
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $path,
            schema(int_schema('id')),
            Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(RowsMother::ids(1, 3), new NoopCodec()))
                . Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(
                    RowsMother::ids(4, 6),
                    new NoopCodec(),
                )),
            6,
        );

        static::assertSame(
            [0, 1, 2, 3],
            array_keys(iterator_to_array(
                (new FloeFiles($filesystem, new AdaptiveBackend()))
                    ->read($path)
                    ->rows(2),
            )),
        );
    }

    public function test_a_frame_of_a_chunk_or_more_is_read_whole_in_one_read(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://large-frames.floe');
        $frame = (new PhpBackend())->encodeFrame(array_to_rows([['s' => str_repeat(
            'x',
            65_536,
        )]], schema(str_schema('s'))), new NoopCodec());
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $path,
            schema(str_schema('s')),
            str_repeat(Format::frame(Format::FRAME_BATCH, $frame), 20),
            20,
        );
        $source = new ClosingSpySourceStream($filesystem->readFrom($path));
        $reader = new FloeStreamReader($source, new NoopCodec(), 4096, new PhpBackend());
        $reader->footer();
        $source->reads = [];

        $rows = iterator_to_array($reader->rows(1), false);

        static::assertCount(20, $rows);
        static::assertSame(str_repeat('x', 65_536), $rows[19]->toArray()[0]['s']);
        static::assertCount(20, array_filter(
            $source->reads,
            static fn(int $length): bool => $length === strlen($frame),
        ));
        static::assertLessThanOrEqual(41, count($source->reads));
    }

    public function test_small_frames_take_no_more_reads_than_chunks_of_the_file(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://small-frames.floe');
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $path,
            schema(int_schema('id')),
            implode('', array_map(
                static fn(int $id): string => Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(
                    RowsMother::ids($id, $id),
                    new NoopCodec(),
                )),
                range(1, 50),
            )),
            50,
        );
        $source = new ClosingSpySourceStream($filesystem->readFrom($path));
        $reader = new FloeStreamReader($source, new NoopCodec(), 256, new PhpBackend());
        $reader->footer();
        $source->reads = [];

        static::assertSame(
            range(1, 50),
            array_merge(...array_map(
                static fn(Rows $batch): array => array_column($batch->toArray(), 'id'),
                iterator_to_array($reader->rows(), false),
            )),
        );
        static::assertLessThanOrEqual(
            (int) ceil(((int) $filesystem->readFrom($path)->size() - Format::HEADER_LENGTH) / 256),
            count($source->reads),
        );
    }

    public function test_a_chunk_of_one_byte_reads_every_frame(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://one-byte.floe');
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $path,
            schema(int_schema('id')),
            Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(RowsMother::ids(1, 3), new NoopCodec()))
                . Format::frame(Format::FRAME_BATCH, (new PhpBackend())->encodeFrame(
                    RowsMother::ids(4, 6),
                    new NoopCodec(),
                )),
            6,
        );

        static::assertSame(
            [
                [['id' => 1], ['id' => 2], ['id' => 3]],
                [['id' => 4], ['id' => 5], ['id' => 6]],
                [['id' => 5], ['id' => 6]],
            ],
            array_map(static fn(Rows $batch) => $batch->toArray(), [
                ...iterator_to_array(
                    (new FloeStreamReader($filesystem->readFrom($path), new NoopCodec(), 1, new PhpBackend()))->rows(),
                    false,
                ),
                ...iterator_to_array(
                    (new FloeStreamReader($filesystem->readFrom($path), new NoopCodec(), 1, new PhpBackend()))->rows(
                        offset: 4,
                    ),
                    false,
                ),
            ]),
        );
    }

    public function test_a_stream_ending_inside_a_frame_header_is_truncated(): void
    {
        $this->expectExceptionObject(new FloeException('Floe stream is truncated, frame header is incomplete'));

        iterator_to_array(FloeStreamReaderContext::truncatedAt(3)->rows(), false);
    }

    public function test_a_stream_ending_inside_a_frame_body_is_truncated(): void
    {
        $this->expectExceptionObject(new FloeException('Floe stream is truncated, frame body is incomplete'));

        iterator_to_array(FloeStreamReaderContext::truncatedAt(-1)->rows(), false);
    }
}

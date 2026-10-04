<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Rows;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeReader;
use Flow\Floe\FloeStreamReader;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\Tests\Context\FloeStreamReaderContext;
use Flow\Floe\Tests\Double\ClosingSpySourceStream;
use Flow\Floe\Tests\Mother\RowsMother;
use PHPUnit\Framework\TestCase;

use function array_keys;
use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FloeStreamReaderTest extends TestCase
{
    public function test_close_closes_the_source_stream(): void
    {
        $filesystem = memory_filesystem();
        $path = path('memory://close.floe');

        $data = array_to_rows([['id' => 1]], schema(int_schema('id')));
        $writer = new FloeWriter($filesystem, $data->schema(), new AdaptiveBackend());
        $writer->create($path);
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
            (new FloeReader($filesystem, new AdaptiveBackend()))
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
            Format::frame(Format::FRAME_BATCH, RowsMother::ids(1, 3)->encodeFrame())
                . Format::frame(Format::FRAME_BATCH, RowsMother::ids(4, 6)->encodeFrame()),
            6,
        );

        static::assertSame(
            [[['id' => 5], ['id' => 6]]],
            array_map(
                static fn(Rows $batch) => $batch->toArray(),
                iterator_to_array(
                    (new FloeReader($filesystem, new AdaptiveBackend()))
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
            Format::frame(Format::FRAME_BATCH, RowsMother::ids(1, 5)->encodeFrame()),
            5,
        );

        static::assertSame(
            [[['id' => 1], ['id' => 2]]],
            array_map(
                static fn(Rows $batch) => $batch->toArray(),
                iterator_to_array(
                    (new FloeReader($filesystem, new AdaptiveBackend()))
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
            Format::frame(Format::FRAME_BATCH, RowsMother::ids(1, 5)->encodeFrame()),
            5,
        );
        FloeStreamReaderContext::writeFrames(
            $filesystem,
            $two,
            schema(int_schema('id')),
            Format::frame(Format::FRAME_BATCH, RowsMother::ids(1, 2)->encodeFrame())
                . Format::frame(Format::FRAME_BATCH, RowsMother::ids(3, 4)->encodeFrame()),
            4,
        );

        static::assertSame(
            [2, 2, 1],
            array_map(
                static fn(Rows $batch) => $batch->count(),
                iterator_to_array(
                    (new FloeReader($filesystem, new AdaptiveBackend()))
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
                    (new FloeReader($filesystem, new AdaptiveBackend()))
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
            Format::frame(Format::FRAME_BATCH, RowsMother::ids(1, 3)->encodeFrame())
                . Format::frame(Format::FRAME_BATCH, RowsMother::ids(4, 6)->encodeFrame()),
            6,
        );

        static::assertSame(
            [0, 1, 2, 3],
            array_keys(iterator_to_array(
                (new FloeReader($filesystem, new AdaptiveBackend()))
                    ->read($path)
                    ->rows(2),
            )),
        );
    }
}

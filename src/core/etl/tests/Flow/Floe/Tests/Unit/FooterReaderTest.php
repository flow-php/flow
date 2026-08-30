<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;
use Flow\Floe\FloeWriter;
use Flow\Floe\FooterReader;
use Flow\Floe\Format;
use Flow\Floe\Tests\Double\ClosingSpySourceStream;
use Flow\Floe\Tests\Double\UnsizedSourceStream;
use PHPUnit\Framework\TestCase;

use function chr;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function ord;
use function str_repeat;

final class FooterReaderTest extends TestCase
{
    public function test_reads_footer_location_of_a_written_file(): void
    {
        $fs = memory_filesystem();
        $path = path('memory://footer.floe');
        $data = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        $writer = new FloeWriter($fs, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $location = (new FooterReader())->read($fs->readFrom($path), new NoopCodec());

        static::assertSame(2, $location->footer->totalRows);
        static::assertGreaterThan(Format::HEADER_LENGTH, $location->footerFrameStart);
        static::assertSame(Format::FRAME_FOOTER, ord($fs->readFrom($path)->read(1, $location->footerFrameStart)));
    }

    public function test_unsized_stream_throws(): void
    {
        $fs = memory_filesystem();
        $path = path('memory://unsized.floe');
        $writer = new FloeWriter($fs, schema());
        $writer->create($path);
        $writer->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('requires a sized stream');

        (new FooterReader())->read(new UnsizedSourceStream($fs->readFrom($path)), new NoopCodec());
    }

    public function test_stream_too_small_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('too small');

        (new FooterReader())->read(new MemorySourceStream('FLOE'), new NoopCodec());
    }

    public function test_codec_mismatch_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('written with codec 0x05, expected 0x00');

        (new FooterReader())->read(
            new MemorySourceStream('FLOE' . chr(Format::VERSION) . "\x05" . str_repeat("\0", 10)),
            new NoopCodec(),
        );
    }

    public function test_footer_that_does_not_fit_throws(): void
    {
        $fs = memory_filesystem();
        $path = path('memory://torn.floe');
        $fs
            ->writeTo($path)
            ->append(Format::header(0x00) . Format::trailer(9999))
            ->close();

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('footer does not fit inside the file');

        (new FooterReader())->read($fs->readFrom($path), new NoopCodec());
    }

    public function test_leaves_the_source_open_on_success(): void
    {
        $fs = memory_filesystem();
        $path = path('memory://close-ok.floe');
        $data = rows(schema(int_schema('id')), row(['id' => 1]));
        $writer = new FloeWriter($fs, $data->schema());
        $writer->create($path);
        $writer->write($data);
        $writer->close();

        $spy = new ClosingSpySourceStream($fs->readFrom($path));

        (new FooterReader())->read($spy, new NoopCodec());

        static::assertSame(0, $spy->closeCount);
    }

    public function test_leaves_the_source_open_on_error(): void
    {
        $spy = new ClosingSpySourceStream(new MemorySourceStream('FLOE'));

        try {
            (new FooterReader())->read($spy, new NoopCodec());
        } catch (FloeException) {
        }

        static::assertSame(0, $spy->closeCount);
    }
}

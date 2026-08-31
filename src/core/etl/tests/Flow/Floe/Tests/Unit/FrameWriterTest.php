<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Filesystem\Stream\StringDestinationStream;
use Flow\Floe\Format;
use Flow\Floe\FrameWriter;
use PHPUnit\Framework\TestCase;

use function chr;
use function Flow\Filesystem\DSL\path;
use function pack;
use function strlen;

final class FrameWriterTest extends TestCase
{
    public function test_header_emits_magic_version_and_codec(): void
    {
        $sink = new StringDestinationStream(path('memory://frame.floe'));
        $writer = new FrameWriter($sink, 0x00);

        $writer->header();
        $writer->flush();

        static::assertSame(Format::header(0x00), $sink->content());
        static::assertSame(Format::HEADER_LENGTH, $writer->position());
    }

    public function test_row_frame_is_type_length_prefixed_body(): void
    {
        $sink = new StringDestinationStream(path('memory://frame.floe'));
        $writer = new FrameWriter($sink, 0x00);

        $writer->row('abc');
        $writer->flush();

        static::assertSame(chr(Format::FRAME_ROW) . pack('V', 3) . 'abc', $sink->content());
        static::assertSame(Format::FRAME_HEADER_LENGTH + 3, $writer->position());
    }

    public function test_footer_appends_trailer_and_flushes(): void
    {
        $sink = new StringDestinationStream(path('memory://frame.floe'));
        $writer = new FrameWriter($sink, 0x00);

        $writer->footer('{"v":1}');

        static::assertSame(
            Format::frame(Format::FRAME_FOOTER, '{"v":1}' . Format::trailer(strlen('{"v":1}'))),
            $sink->content(),
        );
    }

    public function test_raw_appends_bytes_verbatim_and_advances_position(): void
    {
        $sink = new StringDestinationStream(path('memory://frame.floe'));
        $writer = new FrameWriter($sink, 0x00);

        $writer->raw('already-framed-bytes');
        $writer->flush();

        static::assertSame('already-framed-bytes', $sink->content());
        static::assertSame(strlen('already-framed-bytes'), $writer->position());
    }

    public function test_start_position_seeds_the_logical_position(): void
    {
        $writer = new FrameWriter(new StringDestinationStream(path('memory://frame.floe')), 0x00, startPosition: 128);

        static::assertSame(128, $writer->position());

        $writer->row('x');

        static::assertSame(128 + Format::FRAME_HEADER_LENGTH + 1, $writer->position());
    }

    public function test_buffer_flushes_when_it_reaches_buffer_size(): void
    {
        $sink = new StringDestinationStream(path('memory://frame.floe'));
        $writer = new FrameWriter($sink, 0x00, bufferSize: 8);

        $writer->raw('1234');
        static::assertSame('', $sink->content());

        $writer->raw('5678');
        static::assertSame('12345678', $sink->content());
    }

    public function test_close_flushes_pending_bytes(): void
    {
        $sink = new StringDestinationStream(path('memory://frame.floe'));
        $writer = new FrameWriter($sink, 0x00);

        $writer->row('pending');
        $writer->close();

        static::assertSame(chr(Format::FRAME_ROW) . pack('V', 7) . 'pending', $sink->content());
    }
}

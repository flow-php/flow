<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;
use Flow\Floe\Tests\Mother\FrameReaderMother;
use PHPUnit\Framework\TestCase;

use function chr;
use function iterator_to_array;
use function pack;
use function str_repeat;

final class FrameReaderTest extends TestCase
{
    public function test_empty_stream_in_lenient_mode_yields_nothing(): void
    {
        static::assertSame([], iterator_to_array(FrameReaderMother::overBytes('')->frames(lenient: true)));
    }

    public function test_empty_stream_in_strict_mode_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('header is incomplete');

        iterator_to_array(FrameReaderMother::overBytes('')->frames());
    }

    public function test_flags_mismatch_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('written with codec 0x05, expected 0x00');

        iterator_to_array(FrameReaderMother::overBytes(Format::header(0x05))->frames());
    }

    public function test_frames_crossing_compaction_threshold_are_streamed(): void
    {
        $body = str_repeat('x', 8192);
        $bytes = Format::header(0x00);

        for ($i = 0; $i < 200; $i++) {
            $bytes .= Format::frame(Format::FRAME_ROW, $body);
        }

        $read = 0;

        foreach (FrameReaderMother::overBytes($bytes, chunkSize: 4096)->frames() as [$type, $frameBody]) {
            static::assertSame(Format::FRAME_ROW, $type);
            static::assertSame($body, $frameBody);
            $read++;
        }

        static::assertSame(200, $read);
    }

    public function test_frames_are_yielded_in_order_with_bodies(): void
    {
        $bytes =
            Format::header(0x00)
            . Format::frame(Format::FRAME_ROW, 'row-1')
            . Format::frame(Format::FRAME_FOOTER, 'footer');

        static::assertSame(
            [
                [Format::FRAME_ROW,    'row-1'],
                [Format::FRAME_FOOTER, 'footer'],
            ],
            iterator_to_array(FrameReaderMother::overBytes($bytes)->frames()),
        );
    }

    public function test_invalid_magic_throws_even_in_lenient_mode(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('magic');

        iterator_to_array(FrameReaderMother::overBytes('NOPE' . "\x01\x00")->frames(lenient: true));
    }

    public function test_stream_ending_at_frame_boundary_ends_cleanly(): void
    {
        $bytes = Format::header(0x00) . Format::frame(Format::FRAME_ROW, 'row');

        static::assertCount(1, iterator_to_array(FrameReaderMother::overBytes($bytes)->frames()));
    }

    public function test_truncated_frame_body_in_lenient_mode_salvages_previous_frames(): void
    {
        $bytes =
            Format::header(0x00)
            . Format::frame(Format::FRAME_ROW, 'complete')
            . chr(Format::FRAME_ROW)
            . pack('V', 100)
            . 'short';

        static::assertSame(
            [[Format::FRAME_ROW, 'complete']],
            iterator_to_array(FrameReaderMother::overBytes($bytes)->frames(lenient: true)),
        );
    }

    public function test_truncated_frame_body_in_strict_mode_throws(): void
    {
        $bytes = Format::header(0x00) . chr(Format::FRAME_ROW) . pack('V', 100) . 'short';

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('frame body is incomplete');

        iterator_to_array(FrameReaderMother::overBytes($bytes)->frames());
    }

    public function test_truncated_frame_header_in_strict_mode_throws(): void
    {
        $bytes = Format::header(0x00) . chr(Format::FRAME_ROW) . 'xy';

        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('frame header is incomplete');

        iterator_to_array(FrameReaderMother::overBytes($bytes)->frames());
    }
}

<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\Exception\FloeException;
use Flow\Floe\Format;
use PHPUnit\Framework\TestCase;

use function chr;
use function pack;
use function strlen;

final class FormatTest extends TestCase
{
    public function test_frame_prefixes_type_and_length(): void
    {
        static::assertSame(chr(Format::FRAME_ROW) . pack('V', 3) . 'abc', Format::frame(Format::FRAME_ROW, 'abc'));
    }

    public function test_header_starts_with_magic_version_and_flags(): void
    {
        $header = Format::header(0x00);

        static::assertSame(Format::HEADER_LENGTH, strlen($header));
        static::assertSame('FLOE' . chr(Format::VERSION) . "\x00", $header);
        static::assertSame(0x00, Format::validateHeader($header));
    }

    public function test_parse_trailer_returns_footer_length(): void
    {
        static::assertSame(1234, Format::parseTrailer(Format::trailer(1234)));
    }

    public function test_parse_trailer_with_unknown_magic_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('magic');

        Format::parseTrailer(pack('V', 10) . 'NOPE');
    }

    public function test_parse_trailer_with_wrong_length_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('exactly 8 bytes');

        Format::parseTrailer('FLOE');
    }

    public function test_trailer_is_footer_length_and_magic(): void
    {
        static::assertSame(pack('V', 42) . 'FLOE', Format::trailer(42));
        static::assertSame(Format::TRAILER_LENGTH, strlen(Format::trailer(42)));
    }

    public function test_validate_codec_id_accepts_noop(): void
    {
        Format::validateCodecId(0x00);

        $this->addToAssertionCount(1);
    }

    public function test_validate_codec_id_rejects_other_codecs(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('supports only the no-op codec');

        Format::validateCodecId(0x01);
    }

    public function test_validate_header_returns_flags(): void
    {
        static::assertSame(0x7F, Format::validateHeader(Format::header(0x7F)));
    }

    public function test_validate_header_with_unknown_magic_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('magic');

        Format::validateHeader('NOPE' . "\x01\x00");
    }

    public function test_validate_header_with_unsupported_version_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('version');

        Format::validateHeader(Format::MAGIC . "\x7F\x00");
    }

    public function test_validate_truncated_header_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('header is incomplete');

        Format::validateHeader('FLO');
    }
}

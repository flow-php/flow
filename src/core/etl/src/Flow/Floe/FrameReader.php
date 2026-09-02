<?php

declare(strict_types=1);

namespace Flow\Floe;

use Closure;
use Flow\Filesystem\SourceStream;
use Flow\Floe\Exception\FloeException;
use Generator;

use function ord;
use function sprintf;
use function strlen;
use function substr;
use function unpack;

final class FrameReader
{
    public const int COMPACT_THRESHOLD = 1_048_576;

    public function __construct(
        private readonly SourceStream $stream,
        private readonly int $expectedFlags,
        private readonly int $chunkSize = 65536,
    ) {}

    /**
     * @param \Generator<int, string> $chunks
     *
     * @return Closure(int): bool
     */
    public static function chunkFiller(string &$buffer, int &$position, Generator $chunks): Closure
    {
        return static function (int $bytes) use (&$buffer, &$position, $chunks): bool {
            while ((strlen($buffer) - $position) < $bytes) {
                if (!$chunks->valid()) {
                    return false;
                }

                $buffer .= $chunks->current();
                $chunks->next();
            }

            return true;
        };
    }

    /**
     * @throws FloeException
     *
     * @return \Generator<int, array{0: int, 1: string}> frame type and frame body
     */
    public function frames(): Generator
    {
        /** @var int<1, max> $chunkSize */
        $chunkSize = $this->chunkSize;
        $chunks = $this->stream->iterate($chunkSize);
        $buffer = '';
        $position = 0;

        $fill = self::chunkFiller($buffer, $position, $chunks);

        if (!$fill(Format::HEADER_LENGTH)) {
            throw new FloeException('Floe stream is truncated, header is incomplete');
        }

        $flags = Format::validateHeader(substr($buffer, 0, Format::HEADER_LENGTH));

        if ($flags !== $this->expectedFlags) {
            throw new FloeException(sprintf(
                'Floe stream was written with codec 0x%02X, expected 0x%02X',
                $flags,
                $this->expectedFlags,
            ));
        }

        $position = Format::HEADER_LENGTH;

        while (true) {
            if (!$fill(Format::FRAME_HEADER_LENGTH)) {
                if ((strlen($buffer) - $position) === 0) {
                    return;
                }

                throw new FloeException('Floe stream is truncated, frame header is incomplete');
            }

            $frameType = ord($buffer[$position]);
            $frameLength = unpack('V', $buffer, $position + 1)[1];

            if (!$fill(Format::FRAME_HEADER_LENGTH + $frameLength)) {
                throw new FloeException('Floe stream is truncated, frame body is incomplete');
            }

            yield [$frameType, substr($buffer, $position + Format::FRAME_HEADER_LENGTH, $frameLength)];

            $position += Format::FRAME_HEADER_LENGTH + $frameLength;

            if ($position >= self::COMPACT_THRESHOLD) {
                $buffer = substr($buffer, $position);
                $position = 0;
            }
        }
    }
}

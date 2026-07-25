<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\Filesystem\DestinationStream;

use function chr;
use function count;
use function pack;
use function strlen;

final class FrameWriter
{
    private string $buffer = '';

    private int $position;

    public function __construct(
        private readonly DestinationStream $stream,
        private readonly int $codecId,
        private readonly int $bufferSize = 65_536,
        int $startPosition = 0,
    ) {
        $this->position = $startPosition;
    }

    public function header(): void
    {
        $this->buffer .= Format::header($this->codecId);
        $this->position += Format::HEADER_LENGTH;
    }

    public function row(string $body): void
    {
        $this->buffer .= chr(Format::FRAME_ROW) . pack('V', strlen($body)) . $body;
        $this->position += Format::FRAME_HEADER_LENGTH + strlen($body);
        $this->flushIfFull();
    }

    /**
     * @param array<string, string> $combo
     */
    public function partitions(array $combo): void
    {
        $body = self::partitionsBody($combo);
        $this->buffer .= Format::frame(Format::FRAME_PARTITIONS, $body);
        $this->position += Format::FRAME_HEADER_LENGTH + strlen($body);
        $this->flushIfFull();
    }

    public function footer(string $footerJson): void
    {
        $this->buffer .= Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson)));
        $this->flush();
    }

    /**
     * Verbatim byte passthrough for the mergeSplice fast path (no re-encode).
     */
    public function raw(string $bytes): void
    {
        $this->buffer .= $bytes;
        $this->position += strlen($bytes);
        $this->flushIfFull();
    }

    public function position(): int
    {
        return $this->position;
    }

    public function flush(): void
    {
        if ($this->buffer !== '') {
            $this->stream->append($this->buffer);
            $this->buffer = '';
        }
    }

    public function close(): void
    {
        $this->flush();
        $this->stream->close();
    }

    /**
     * @param array<string, string> $combo
     */
    private static function partitionsBody(array $combo): string
    {
        $body = pack('V', count($combo));

        foreach ($combo as $name => $value) {
            $body .= pack('V', strlen($name)) . $name . pack('V', strlen($value)) . $value;
        }

        return $body;
    }

    private function flushIfFull(): void
    {
        if (strlen($this->buffer) >= $this->bufferSize) {
            $this->flush();
        }
    }
}

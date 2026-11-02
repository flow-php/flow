<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Floe;

use Flow\Filesystem\DestinationStream;

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

    public function frame(int $type, string $body): void
    {
        $this->position += Format::FRAME_HEADER_LENGTH + strlen($body);

        // a body past the buffer goes to the stream as it is, so it is never copied into the buffer
        if (strlen($body) >= $this->bufferSize) {
            $this->flush();
            $this->stream->append(Format::frameHeader($type, strlen($body)));
            $this->stream->append($body);

            return;
        }

        $this->buffer .= Format::frame($type, $body);
        $this->flushIfFull();
    }

    public function footer(string $footerJson): void
    {
        $this->buffer .= Format::frame(Format::FRAME_FOOTER, $footerJson . Format::trailer(strlen($footerJson)));
        $this->flush();
    }

    /**
     * Complete frames the caller already built, appended as they are.
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

    private function flushIfFull(): void
    {
        if (strlen($this->buffer) >= $this->bufferSize) {
            $this->flush();
        }
    }
}

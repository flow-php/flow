<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;

final class JsonOpenSink implements FileSink
{
    private bool $touched = false;

    private bool $written = false;

    public function __construct(
        private readonly DestinationStream $stream,
        private readonly JsonEncoder $encoder,
        private readonly JsonFraming $framing,
    ) {}

    public function close(): void
    {
        if ($this->written) {
            $this->stream->append($this->framing->closing());
        } elseif ($this->touched) {
            $this->stream->append($this->framing->empty());
        }
    }

    public function write(Rows $rows): void
    {
        $this->touched = true;

        if ($rows->count() === 0) {
            return;
        }

        $this->stream->append(
            ($this->written ? $this->framing->separator() : $this->framing->opening())
                . $this->encoder->encode($rows, $this->framing->separator()),
        );
        $this->written = true;
    }
}

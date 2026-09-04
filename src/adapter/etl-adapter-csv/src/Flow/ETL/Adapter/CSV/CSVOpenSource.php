<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Row\RawRowValues;
use Flow\Filesystem\SourceStream;
use Generator;

final readonly class CSVOpenSource
{
    public function __construct(
        public SourceStream $stream,
        public CSVDialect $dialect,
        public CSVEncoder $encoder,
        public CSVLineReader $lineReader,
    ) {}

    public function close(): void
    {
        $this->stream->close();
    }

    /**
     * This instance is consumed afterwards.
     *
     * @return list<string>
     */
    public function columns(): array
    {
        foreach ($this->lineReader->readLines($this->stream) as $line) {
            $this->encoder->decode([$line]);

            break;
        }

        return $this->encoder->headers() ?? [];
    }

    /**
     * CSVLineReader::readLines() already joins a quoted multi-line record, so never re-split or re-join here.
     * This instance is consumed afterwards.
     *
     * @return Generator<int, RawRowValues>
     */
    public function records(): Generator
    {
        foreach ($this->lineReader->readLines($this->stream) as $line) {
            foreach ($this->encoder->decode([$line]) as $values) {
                yield $values;
            }
        }
    }
}

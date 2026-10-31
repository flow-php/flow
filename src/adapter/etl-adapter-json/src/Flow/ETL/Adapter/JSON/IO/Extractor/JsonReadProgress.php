<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Extractor;

final class JsonReadProgress
{
    private ?int $lastLine = null;

    private int $lastOffset = 0;

    private ?string $lastRecord = null;

    /**
     * @var int<0, max>
     */
    private int $total = 0;

    /**
     * @param int<0, max> $bytes
     */
    public function add(int $bytes): void
    {
        $this->total += $bytes;
    }

    /**
     * The 1-based line of the record read last; null unless the read started at the file's first byte.
     */
    public function lastLine(): ?int
    {
        return $this->lastLine;
    }

    /**
     * The byte offset of the line read last, or the 1-based number of the document element read last.
     */
    public function lastOffset(): int
    {
        return $this->lastOffset;
    }

    /**
     * The text of the line read last; null for a document element.
     */
    public function lastRecord(): ?string
    {
        return $this->lastRecord;
    }

    public function recordAt(int $offset, ?int $line, ?string $record): void
    {
        $this->lastOffset = $offset;
        $this->lastLine = $line;
        $this->lastRecord = $record;
    }

    /**
     * @return int<0, max>
     */
    public function total(): int
    {
        return $this->total;
    }
}

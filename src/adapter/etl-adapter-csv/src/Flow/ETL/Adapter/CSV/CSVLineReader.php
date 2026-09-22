<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\Filesystem\SourceStream;
use Generator;

use function rtrim;
use function str_contains;
use function str_starts_with;
use function strlen;
use function substr;

final class CSVLineReader
{
    private readonly CSVRecordBoundary $boundary;

    /**
     * @var int<0, max>
     */
    private int $lastRecordBytes = 0;

    /**
     * @param null|int<1, max> $charactersReadInLine
     */
    public function __construct(
        private readonly string $enclosure,
        string $separator = ',',
        string $escape = '\\',
        private readonly ?int $charactersReadInLine = null,
        private readonly bool $removeBOM = true,
    ) {
        $this->boundary = new CSVRecordBoundary($enclosure, $separator, $escape);
    }

    /**
     * The bytes the record readLines() yielded last took in the stream, line endings and BOM included.
     * SourceStream::readLines() cannot tell whether the last line ended with "\n", so a last record without one is
     * counted one byte too long.
     *
     * @return int<0, max>
     */
    public function lastRecordBytes(): int
    {
        return $this->lastRecordBytes;
    }

    /**
     * @return \Generator<int, string>
     */
    public function readLines(SourceStream $stream): Generator
    {
        $lineNumber = 0;
        $buffer = '';
        $bytes = 0;

        foreach ($stream->readLines(length: $this->charactersReadInLine) as $rawLine) {
            $buffer .= $rawLine;
            $bytes += strlen($rawLine) + 1;
            $this->lastRecordBytes = $bytes;

            if (!str_contains($buffer, $this->enclosure)) {
                yield $this->removeBOM && $lineNumber === 0
                    ? $this->removeBOMFromLine(rtrim($buffer, "\r\n"))
                    : rtrim($buffer, "\r\n");
                $lineNumber++;
                $buffer = '';
                $bytes = 0;
            } else {
                if ($this->boundary->isComplete($buffer)) {
                    yield $this->removeBOM && $lineNumber === 0
                        ? $this->removeBOMFromLine(rtrim($buffer, "\r\n"))
                        : rtrim($buffer, "\r\n");
                    $lineNumber++;
                    $buffer = '';
                    $bytes = 0;
                } else {
                    $buffer .= "\n";
                }
            }
        }

        if ($buffer !== '') {
            yield $this->removeBOM && $lineNumber === 0
                ? $this->removeBOMFromLine(rtrim($buffer, "\r\n"))
                : rtrim($buffer, "\r\n");
        }
    }

    /**
     * Remove Byte Order Mark (BOM) from the beginning of a line if present.
     */
    private function removeBOMFromLine(string $line): string
    {
        if (str_starts_with($line, "\xEF\xBB\xBF")) {
            return substr($line, 3);
        }

        if (str_starts_with($line, "\xFF\xFE\x00\x00")) {
            return substr($line, 4);
        }

        if (str_starts_with($line, "\x00\x00\xFE\xFF")) {
            return substr($line, 4);
        }

        if (str_starts_with($line, "\xFF\xFE")) {
            return substr($line, 2);
        }

        if (str_starts_with($line, "\xFE\xFF")) {
            return substr($line, 2);
        }

        return $line;
    }
}

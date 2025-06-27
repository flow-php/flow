<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\Filesystem\SourceStream;

final readonly class CSVLineReader
{
    /**
     * @param null|int<1, max> $charactersReadInLine
     */
    public function __construct(
        private string $enclosure,
        private ?int $charactersReadInLine = null,
        private bool $removeBOM = true,
    ) {
    }

    /**
     * @return \Generator<int, string>
     */
    public function readLines(SourceStream $stream) : \Generator
    {
        $lineNumber = 0;
        $buffer = '';

        foreach ($stream->readLines(length: $this->charactersReadInLine) as $rawLine) {
            $buffer .= $rawLine;

            if (!\str_contains($buffer, $this->enclosure)) {
                yield $this->removeBOM && $lineNumber === 0 ? $this->removeBOMFromLine(\rtrim($buffer, "\r\n")) : rtrim($buffer, "\r\n");
                $lineNumber++;
                $buffer = '';
            } else {
                if ($this->isCompleteCSVRecord($buffer)) {
                    yield $this->removeBOM && $lineNumber === 0 ? $this->removeBOMFromLine(\rtrim($buffer, "\r\n")) : \rtrim($buffer, "\r\n");
                    $lineNumber++;
                    $buffer = '';
                } else {
                    $buffer .= "\n";
                }
            }
        }

        if ($buffer !== '') {
            yield $this->removeBOM && $lineNumber === 0 ? $this->removeBOMFromLine(\rtrim($buffer, "\r\n")) : \rtrim($buffer, "\r\n");
        }
    }

    /**
     * Check if the current buffer contains a complete CSV record
     * by counting enclosures and ensuring they are properly paired.
     */
    private function isCompleteCSVRecord(string $buffer) : bool
    {
        if (!\str_contains($buffer, $this->enclosure)) {
            return true;
        }

        return \substr_count($buffer, $this->enclosure) % 2 === 0;
    }

    /**
     * Remove Byte Order Mark (BOM) from the beginning of a line if present.
     */
    private function removeBOMFromLine(string $line) : string
    {
        if (\str_starts_with($line, "\xEF\xBB\xBF")) {
            return \substr($line, 3);
        }

        if (\str_starts_with($line, "\xFF\xFE\x00\x00")) {
            return \substr($line, 4);
        }

        if (\str_starts_with($line, "\x00\x00\xFE\xFF")) {
            return \substr($line, 4);
        }

        if (\str_starts_with($line, "\xFF\xFE")) {
            return \substr($line, 2);
        }

        if (\str_starts_with($line, "\xFE\xFF")) {
            return \substr($line, 2);
        }

        return $line;
    }
}

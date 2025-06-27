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
                yield $lineNumber => rtrim($buffer, "\r\n");
                $lineNumber++;
                $buffer = '';
            } else {
                if ($this->isCompleteCSVRecord($buffer)) {
                    yield $lineNumber => \rtrim($buffer, "\r\n");
                    $lineNumber++;
                    $buffer = '';
                } else {
                    $buffer .= "\n";
                }
            }
        }

        if ($buffer !== '') {
            yield $lineNumber => \rtrim($buffer, "\r\n");
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
}

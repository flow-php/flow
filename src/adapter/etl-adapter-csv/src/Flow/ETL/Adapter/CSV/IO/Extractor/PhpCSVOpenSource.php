<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RecordBatch;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\RangeSourceStream;
use Flow\Types\Type\TypeNarrower;
use Generator;

use function is_array;
use function is_string;
use function substr_count;

final class PhpCSVOpenSource implements CSVOpenSource
{
    /**
     * @var int<0, max>
     */
    private int $producedBytes = 0;

    /**
     * @var int<0, max>
     */
    private int $producedRows = 0;

    public function __construct(
        private readonly SourceStream $stream,
        private readonly CSVDecoder $decoder,
        private readonly CSVLineReader $lineReader,
    ) {}

    /**
     * @return Generator<int, CorruptedRecordException|Rows>
     */
    public function batches(Schema $schema, int $batchSize, Backend $backend, ?int $sampleSize = null): Generator
    {
        $uri = $this->stream->path()->uri();
        $position = $this->stream instanceof RangeSourceStream ? $this->stream->start() : 0;
        // a window cut past the file's start cannot know how many lines came before it
        $line = $position === 0 ? 1 : null;
        $batch = new RecordBatch($schema, $backend, $batchSize, $uri, $sampleSize);

        foreach ($this->lineReader->readLines($this->stream) as $record) {
            $offset = $position;
            $recordLine = $line;
            $position += $this->lineReader->lastRecordBytes();

            if ($line !== null) {
                $line += 1 + substr_count($record, "\n");
            }

            $cells = $this->decoder->cells($record);
            $values = $this->decoder->decode($cells);

            if ($values === null) {
                continue;
            }

            if (is_string($values)) {
                yield from $batch->refuse(CorruptedRecordException::fromCells(
                    $uri,
                    $offset,
                    $recordLine,
                    $values,
                    $cells,
                    $record,
                ));

                continue;
            }

            $this->producedBytes += $this->lineReader->lastRecordBytes();
            $this->producedRows++;

            yield from $batch->add($values, $offset, $recordLine, $record, $cells);
        }

        yield from $batch->flush();
    }

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
            $this->decoder->decode($this->decoder->cells($line));

            if ($this->decoder->headers() !== null) {
                break;
            }
        }

        return $this->decoder->headers() ?? [];
    }

    public function headers(): array
    {
        return $this->decoder->headers() ?? [];
    }

    /**
     * CSVLineReader::readLines() already joins a quoted multi-line record, so never re-split or re-join here.
     * This instance is consumed afterwards.
     *
     * @return Generator<int, array<array-key, ?string>>
     */
    public function records(): Generator
    {
        foreach ($this->lineReader->readLines($this->stream) as $line) {
            $values = $this->decoder->decode($this->decoder->cells($line));

            if (!is_array($values)) {
                continue;
            }

            $this->producedBytes += $this->lineReader->lastRecordBytes();
            $this->producedRows++;

            yield $values;
        }
    }

    public function producedBytes(): int
    {
        return $this->producedBytes;
    }

    public function producedRows(): int
    {
        return $this->producedRows;
    }

    public function sniff(array $names, int $rowBudget, TypeNarrower $typer): ColumnTypes
    {
        return ColumnTypes::sniff($names, $this->records(), $rowBudget, $typer);
    }
}

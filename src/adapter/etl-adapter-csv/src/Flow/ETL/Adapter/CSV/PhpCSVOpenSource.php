<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SchemaInferrer;
use Flow\Filesystem\SourceStream;
use Flow\Types\Type\TypeNarrower;
use Generator;

use function count;

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

    public function batches(Schema $schema, int $batchSize, Backend $backend): Generator
    {
        $rows = [];

        foreach ($this->records() as $values) {
            $rows[] = $values;

            if (count($rows) >= $batchSize) {
                yield (new RowsBuilder($schema, $backend))
                    ->appendRows($rows)
                    ->finish();
                $rows = [];
            }
        }

        if ($rows !== []) {
            yield (new RowsBuilder($schema, $backend))
                ->appendRows($rows)
                ->finish();
        }
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
            $this->decoder->decode([$line]);

            break;
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
            foreach ($this->decoder->decode([$line]) as $values) {
                $this->producedBytes += $this->lineReader->lastRecordBytes();
                $this->producedRows++;

                yield $values;
            }
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

    public function sniff(array $names, int $rowBudget, SchemaInference $inference, TypeNarrower $typer): ColumnTypes
    {
        return (new SchemaInferrer($inference, $typer))->sniff($names, $this->records(), $rowBudget);
    }
}

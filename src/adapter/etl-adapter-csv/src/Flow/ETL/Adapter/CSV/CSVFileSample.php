<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Inference\ColumnTypes;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\SniffsColumnTypes;
use Flow\Types\Type\TypeNarrower;
use Generator;
use IteratorAggregate;

/**
 * @implements IteratorAggregate<int, RawRowValues>
 */
final class CSVFileSample implements IteratorAggregate, SniffsColumnTypes
{
    private ?CSVSampledRows $sampled = null;

    public function __construct(
        private readonly CSVSourceOpener $opener,
        private readonly SourceFile $source,
    ) {}

    /**
     * The source is opened on the first advance; abandoning the generator closes it.
     *
     * @return Generator<int, RawRowValues>
     */
    public function getIterator(): Generator
    {
        $open = $this->opener->open($this->source);

        try {
            yield from $open->records();
        } finally {
            $open->close();
        }
    }

    public function sniffColumnTypes(
        array $names,
        int $rowBudget,
        SchemaInference $inference,
        TypeNarrower $typer,
    ): ColumnTypes {
        $open = $this->opener->open($this->source);

        try {
            $columnTypes = $open->sniff($names, $rowBudget, $inference, $typer);

            // the byte count stays out of ColumnTypes: SniffsColumnTypes is an inference contract, statistics are not
            $this->sampled = new CSVSampledRows(
                $open->producedRows(),
                $open->producedBytes(),
                $rowBudget === -1 || $columnTypes->rows() < $rowBudget,
            );

            return $columnTypes;
        } finally {
            $open->close();
        }
    }

    /**
     * What the last sniffColumnTypes() read; null until one ran.
     */
    public function sampled(): ?CSVSampledRows
    {
        return $this->sampled;
    }
}

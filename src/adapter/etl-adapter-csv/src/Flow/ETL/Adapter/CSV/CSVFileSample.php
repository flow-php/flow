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
final readonly class CSVFileSample implements IteratorAggregate, SniffsColumnTypes
{
    public function __construct(
        private CSVSourceOpener $opener,
        private SourceFile $source,
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
            return $open->sniff($names, $rowBudget, $inference, $typer);
        } finally {
            $open->close();
        }
    }
}

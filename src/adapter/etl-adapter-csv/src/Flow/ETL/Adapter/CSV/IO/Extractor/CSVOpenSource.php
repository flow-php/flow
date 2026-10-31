<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\Inference\ColumnTypes;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Types\Type\TypeNarrower;
use Iterator;

use function interface_exists;

if (interface_exists(CSVOpenSource::class, false)) {
    return;
}

interface CSVOpenSource
{
    public function close(): void;

    /**
     * The bytes, as read, of the rows producedRows() counts: line endings included, the header record excluded.
     *
     * @return int<0, max>
     */
    public function producedBytes(): int;

    /**
     * Rows read from the source so far by records() or sniff() - it may be more than a consumer took.
     *
     * @return int<0, max>
     */
    public function producedRows(): int;

    /**
     * This instance is consumed afterwards.
     *
     * @return list<string>
     */
    public function columns(): array;

    /**
     * Batches of exactly $batchSize rows (the last may be shorter), keyed and ordered by $schema, and every corrupted
     * record handed up in record order. This instance is consumed afterwards.
     *
     * @param int<1, max> $batchSize
     * @param null|int $sampleSize the inference's sample size when $schema was inferred; null when it was declared
     *
     * @throws InferredSchemaException|SchemaMismatchException a refused record under an inferred schema
     *
     * @return Iterator<int, CorruptedRecordException|Rows>
     */
    public function batches(Schema $schema, int $batchSize, Backend $backend, ?int $sampleSize = null): Iterator;

    /**
     * The header records() resolved; [] before it ran or for a 0-byte source.
     *
     * @return list<string>
     */
    public function headers(): array;

    /**
     * The rows of the records that are not corrupted. This instance is consumed afterwards.
     *
     * @return Iterator<int, array<array-key, ?string>>
     */
    public function records(): Iterator;

    /**
     * ColumnTypes::sniff() over records(). This instance is consumed afterwards.
     *
     * @param list<string> $names
     * @param int<0, max>|-1 $rowBudget
     */
    public function sniff(array $names, int $rowBudget, TypeNarrower $typer): ColumnTypes;
}

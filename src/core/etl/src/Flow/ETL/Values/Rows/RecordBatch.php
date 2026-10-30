<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Rows;

use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

use function count;

final class RecordBatch
{
    private int $offset = 0;

    /**
     * @var list<array<array-key, mixed>>
     */
    private array $records = [];

    /**
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private readonly Schema $schema,
        private readonly Backend $backend,
        private readonly int $batchSize,
    ) {}

    /**
     * @param array<array-key, mixed> $record
     *
     * @throws SchemaMismatchException at the refused row's position among every record added so far
     *
     * @return null|Rows a full batch once $batchSize records are held
     */
    public function add(array $record): ?Rows
    {
        $this->records[] = $record;

        return count($this->records) < $this->batchSize ? null : $this->flush();
    }

    /**
     * @throws SchemaMismatchException at the refused row's position among every record added so far
     *
     * @return null|Rows the held records, null when none
     */
    public function flush(): ?Rows
    {
        if ($this->records === []) {
            return null;
        }

        try {
            $rows = (new RowsBuilder($this->schema, $this->backend))
                ->appendRows($this->records)
                ->finish();
        } catch (SchemaMismatchException $mismatch) {
            throw new SchemaMismatchException($this->offset + $mismatch->rowIndex, $mismatch->cause);
        }

        $this->offset += count($this->records);
        $this->records = [];

        return $rows;
    }
}

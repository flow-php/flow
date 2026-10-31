<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Rows;

use Flow\ETL\Foundation\Exception\ColumnMismatchException;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\ColumnName;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function array_diff_key;
use function array_key_exists;
use function array_keys;
use function count;
use function is_int;
use function usort;

final class RecordBatch
{
    /**
     * @var list<null|array<array-key, mixed>>
     */
    private array $fields = [];

    /**
     * @var list<?int>
     */
    private array $lines = [];

    private int $offset = 0;

    /**
     * @var list<int>
     */
    private array $offsets = [];

    /**
     * @var list<?string>
     */
    private array $raws = [];

    /**
     * @var list<array<array-key, mixed>>
     */
    private array $records = [];

    /**
     * @var list<CorruptedRecordException>
     */
    private array $refused = [];

    /**
     * @var null|array<string, mixed>
     */
    private readonly ?array $known;

    /**
     * @param int<1, max> $batchSize
     * @param null|string $uri the file the records come from; null for a source that is not a file
     * @param null|int $sampleSize the inference's sample size when the schema was inferred, -1 for every row; null for a
     *                             declared schema
     * @param null|Schema $known a keyed reader's (JSON, array, sequence) full schema, $schema's columns and those it
     *                           leaves out; under an inferred schema a key it lacks was first seen past the sample.
     *                           A grid reader passes none: its header is checked once per file
     */
    public function __construct(
        private readonly Schema $schema,
        private readonly Backend $backend,
        private readonly int $batchSize,
        private readonly ?string $uri = null,
        private readonly ?int $sampleSize = null,
        ?Schema $known = null,
    ) {
        $this->known = $known?->definitions();
    }

    /**
     * @param array<array-key, mixed> $record
     * @param null|string $raw the record as the source holds it; null when the source holds no text of it
     * @param null|array<array-key, mixed> $fields the cells a grid reader split the record into, before keying them by header
     *
     * @throws InferredSchemaException|SchemaMismatchException a refused record under an inferred schema
     *
     * @return list<CorruptedRecordException|Rows> a full batch's refused records, then its rows; [] while the batch
     *                                             is filling
     */
    public function add(
        array $record,
        int $offset,
        ?int $line = null,
        ?string $raw = null,
        ?array $fields = null,
    ): array {
        $this->records[] = $record;
        $this->offsets[] = $offset;
        $this->lines[] = $line;
        $this->raws[] = $raw;
        $this->fields[] = $fields;

        return (count($this->records) + count($this->refused)) < $this->batchSize ? [] : $this->flush();
    }

    /**
     * A record its reader refused before it became values: it takes its place in the batch, so the batch hands its
     * refused records out in source order.
     *
     * @return list<CorruptedRecordException|Rows> as add()
     */
    public function refuse(CorruptedRecordException $corrupted): array
    {
        $this->refused[] = $corrupted;

        return (count($this->records) + count($this->refused)) < $this->batchSize ? [] : $this->flush();
    }

    /**
     * $records added in order, numbered from 1, then flushed.
     *
     * @param iterable<array<array-key, mixed>> $records
     *
     * @throws InferredSchemaException|SchemaMismatchException a refused record under an inferred schema
     *
     * @return Generator<int, CorruptedRecordException|Rows>
     */
    public function of(iterable $records): Generator
    {
        $number = 0;

        foreach ($records as $record) {
            yield from $this->add($record, ++$number);
        }

        yield from $this->flush();
    }

    /**
     * Under a declared schema a record with a value that does not cast is a corrupted record, handed up and left out
     * of the rows; under an inferred schema it is a schema error.
     *
     * @throws InferredSchemaException|SchemaMismatchException a refused record under an inferred schema
     *
     * @return list<CorruptedRecordException|Rows> the records refused, in the order they were added, then the rows of
     *                                             the rest; [] when none
     */
    public function flush(): array
    {
        $known = $this->known;
        $sampleSize = $this->sampleSize;

        if ($sampleSize !== null && $known !== null) {
            $columnName = new ColumnName();

            foreach ($this->records as $index => $record) {
                foreach (array_keys(array_diff_key($record, $known)) as $key) {
                    // an int key is a positional column (ColumnName), or a numeric-string name PHP turned into an int
                    if (is_int($key) && array_key_exists($columnName->of($key), $known)) {
                        continue;
                    }

                    throw $this->outsideTheSample(
                        new SchemaMismatchException($index, ColumnMismatchException::unexpectedColumn((string) $key)),
                        $sampleSize,
                    );
                }
            }
        }

        if ($this->records === []) {
            $refused = $this->refused;
            $this->clear();

            return $refused;
        }

        try {
            $rows = (new RowsBuilder($this->schema, $this->backend))
                ->appendRows($this->records)
                ->finish();
        } catch (SchemaMismatchException $mismatch) {
            if ($sampleSize !== null) {
                throw $this->outsideTheSample($mismatch, $sampleSize);
            }

            return $this->recordByRecord();
        }

        $handed = [...$this->refused, $rows];
        $this->offset += count($this->records);
        $this->clear();

        return $handed;
    }

    /**
     * A file's refusal names the record; a source that is not a file is named by its reader, which places the refusal
     * by its position among every record added.
     */
    private function outsideTheSample(
        SchemaMismatchException $mismatch,
        int $sampleSize,
    ): InferredSchemaException|SchemaMismatchException {
        $index = $mismatch->rowIndex;

        return $this->uri === null
            ? new SchemaMismatchException($this->offset + $index, $mismatch->cause)
            : InferredSchemaException::outsideTheSample(
                $this->uri,
                $this->offsets[$index],
                $this->lines[$index],
                $sampleSize,
                $mismatch,
            );
    }

    private function clear(): void
    {
        $this->records = [];
        $this->offsets = [];
        $this->lines = [];
        $this->raws = [];
        $this->fields = [];
        $this->refused = [];
    }

    private function corrupted(int $index, ColumnMismatchException $cause): CorruptedRecordException
    {
        return $this->fields[$index] === null
            ? CorruptedRecordException::fromValues(
                $this->uri,
                $this->offsets[$index],
                $this->lines[$index],
                $cause->reason(),
                $this->records[$index],
                $this->raws[$index],
            )
            : CorruptedRecordException::fromCells(
                $this->uri,
                $this->offsets[$index],
                $this->lines[$index],
                $cause->reason(),
                $this->fields[$index],
                $this->raws[$index],
            );
    }

    /**
     * A batch with a refused record, in one pass: each record is tried alone, then the accepted ones are built at once.
     * A refused record leaves the values of its other columns in the trial builder, so a refusal starts a new one. The
     * records refused here and by refuse() leave in source order.
     *
     * @return list<CorruptedRecordException|Rows>
     */
    private function recordByRecord(): array
    {
        $handed = $this->refused;
        $accepted = [];
        $trial = new RowsBuilder($this->schema, $this->backend);

        foreach ($this->records as $index => $record) {
            try {
                $trial->appendRows([$record]);
                $accepted[] = $record;
            } catch (SchemaMismatchException $mismatch) {
                $handed[] = $this->corrupted($index, $mismatch->cause);
                $trial = new RowsBuilder($this->schema, $this->backend);
            }
        }

        usort(
            $handed,
            static fn(CorruptedRecordException $a, CorruptedRecordException $b): int => $a->offset <=> $b->offset,
        );

        if ($accepted !== []) {
            $handed[] = (new RowsBuilder($this->schema, $this->backend))
                ->appendRows($accepted)
                ->finish();
        }

        $this->offset += count($this->records);
        $this->clear();

        return $handed;
    }
}

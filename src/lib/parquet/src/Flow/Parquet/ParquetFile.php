<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Composer\InstalledVersions;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Evaluation\FilterEvaluator;
use Flow\Parquet\ParquetFile\Index\Indexes;
use Flow\Parquet\ParquetFile\Index\StreamIndexes;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\Page\ColumnPageHeader;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Reader\ColumnChunkViewer;
use Flow\Parquet\Reader\FilteredBatches;
use Flow\Parquet\Reader\ReadPlan;
use Generator;

use function array_keys;
use function array_map;
use function array_values;
use function count;

/**
 * @template-covariant R of ParquetFileReader
 */
final class ParquetFile
{
    public const string PARQUET_MAGIC_NUMBER = 'PAR1';

    private const int BATCH_SIZE = 1024;

    private bool $closed = false;

    private ?Indexes $indexes = null;

    /**
     * @param R $reader opened on $stream
     */
    public function __construct(
        private readonly SourceStream $stream,
        private readonly Options $options,
        private readonly ParquetFileReader $reader,
    ) {}

    public function __destruct()
    {
        $this->close();
    }

    public static function createdBy(): string
    {
        return 'flow-php parquet version ' . InstalledVersions::getRootPackage()['pretty_version'];
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->reader->close();
    }

    /**
     * The page indexes, read from the file on first access.
     */
    public function indexes(): Indexes
    {
        return $this->indexes ??= new StreamIndexes($this->reader->footer(), $this->stream);
    }

    public function metadata(): Metadata
    {
        return $this->reader->footer()->metadata();
    }

    public function options(): Options
    {
        return $this->options;
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    public function pageHeaders(): Generator
    {
        foreach ($this->schema()->columnsFlat() as $column) {
            foreach ($this->viewChunksPages($column) as $pageHeader) {
                yield $pageHeader;
            }
        }
    }

    /**
     * @return R
     */
    public function reader(): ParquetFileReader
    {
        return $this->reader;
    }

    public function schema(): Schema
    {
        return $this->reader->footer()->schema();
    }

    /**
     * The reader keeps the rows of the filter's bound conjuncts, the rest are evaluated here; offset and limit count the
     * rows returned.
     *
     * @return Generator<int, array<string, list<mixed>>>
     */
    public function columns(Query $query = new Query(), int $batchSize = self::BATCH_SIZE): Generator
    {
        if ($batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0');
        }

        $schema = $this->schema();
        $columns = $query->columns === []
            ? array_values(array_map(static fn(Column $column): string => $column->name(), $schema->columns()))
            : $query->columns;

        foreach ($columns as $column) {
            if (!$schema->has($column)) {
                throw new InvalidArgumentException("Column \"{$column}\" does not exist");
            }
        }

        $filter = $query->filter === null ? null : BoundFilter::bind($query->filter, $schema, $this->options);
        // a filtered plan counts matching rows from the start, so the offset is applied here; it takes the limit only
        // when it answers every conjunct, otherwise the residual ones below decide which rows count
        $plan = ReadPlan::forFilter(
            $this->reader->footer(),
            $this->indexes(),
            $filter,
            $filter === null ? $query->offset : 0,
            match (true) {
                $filter === null => $query->limit,
                $filter->answersEveryConjunct() && $query->limit !== null => $query->offset + $query->limit,
                default => null,
            },
        );

        if ($filter === null) {
            yield from $this->reader->readColumns($columns, $batchSize, $plan);

            return;
        }

        $residual = $filter->residual();
        $filtered = new FilteredBatches(
            $residual === null ? null : new FilterEvaluator($residual),
            $columns,
            $query->offset,
            $query->limit,
            $batchSize,
        );

        yield from $filtered->filter($this->reader->readColumns($filtered->columns(), $batchSize, $plan));
    }

    /**
     * The query's rows one by one, keyed by column.
     *
     * @return Generator<int, array<string, mixed>>
     */
    public function values(Query $query = new Query()): Generator
    {
        foreach ($this->columns($query) as $chunk) {
            $names = array_keys($chunk);
            $count = count($chunk[$names[0]]);

            for ($i = 0; $i < $count; $i++) {
                $row = [];

                foreach ($names as $name) {
                    $row[$name] = $chunk[$name][$i];
                }

                yield $row;
            }
        }
    }

    /**
     * @return \Generator<ColumnPageHeader>
     */
    private function viewChunksPages(FlatColumn $column): Generator
    {
        $viewer = new ColumnChunkViewer($this->options);

        foreach ($this->metadata()->rowGroups()->all() as $rowGroup) {
            $columnChunk = $rowGroup->getColumnChunk($column);

            foreach ($viewer->view($columnChunk, $this->stream) as $pageHeader) {
                yield new ColumnPageHeader($column, $columnChunk, $pageHeader);
            }
        }
    }
}

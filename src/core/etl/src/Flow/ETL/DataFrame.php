<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Config\Grouping\GroupByAlgorithmBuilder;
use Flow\ETL\Config\Join\JoinAlgorithmBuilder;
use Flow\ETL\Config\Sort\SortAlgorithmBuilder;
use Flow\ETL\DataFrame\GroupedDataFrame;
use Flow\ETL\Dataset\Report;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Execution\Run;
use Flow\ETL\Execution\StatisticsCollector;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Function\AggregatingFunction;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Snapshot;
use Flow\ETL\Row\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\SchemaFormatter;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\Rename\RenameEntryStrategy;
use Generator;

use function array_unshift;
use function array_values;
use function count;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\to_output;
use function is_string;

/**
 * @type Aggregations      = list<AggregatingFunction>
 * @type GroupByReferences = list<string|Reference>
 * @type SortReferences    = list<string|Reference>
 *
 * @import-type Sinks from LogicalPlan
 */
final class DataFrame
{
    private readonly FlowContext $context;

    private Node $cursor;

    /**
     * @var Sinks
     */
    private array $sinks = [];

    private readonly Run $run;

    public function __construct(Extractor $extractor, Config|FlowContext $context)
    {
        $this->context = $context instanceof FlowContext ? $context : new FlowContext($context);
        $this->cursor = new Node\Read($extractor);
        $this->run = Run::in($this->context->config);
    }

    /**
     * @lazy
     */
    public function addRowIndex(string $column = 'index', StartFrom $startFrom = StartFrom::ZERO): self
    {
        $this->cursor = new Node\RowIndex($this->cursor, $column, $startFrom);

        return $this;
    }

    /**
     * @lazy
     *
     * @param Aggregations $aggregations
     * @param null|GroupByAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function aggregate(array $aggregations, ?GroupByAlgorithmBuilder $algorithm = null): self
    {
        $groupBy = new GroupBy();
        $groupBy->aggregate(...$aggregations);

        $this->registerGroupBy($groupBy, $algorithm);

        return $this;
    }

    /**
     * Merge/Split Rows yielded by Extractor into batches but keep those with common value in given column together.
     * This works properly only on sorted datasets.
     *
     * When minSize is not provided, batches will be created only when there is a change in value of the column.
     * When minSize is provided, batches will be created only when there is a change in value of the column or
     * when there are at least minSize rows in the batch.
     *
     * @param Reference|string $column - column to group by (all rows with same value stay together)
     * @param null|int<1, max> $minSize - optional minimum rows per batch for efficiency
     *
     * @lazy
     *
     * @throws InvalidArgumentException
     */
    public function batchBy(string|Reference $column, ?int $minSize = null): self
    {
        $this->cursor = new Node\BatchBy($this->cursor, UnresolvedReference::init($column), $minSize);

        return $this;
    }

    /**
     * Merge/Split Rows yielded by Extractor into batches of given size.
     * For example, when Extractor is yielding one row at time, this method will merge them into batches of given size
     * before passing them to the next pipeline element.
     * Similarly when Extractor is yielding batches of rows, this method will split them into smaller batches of given
     * size.
     *
     * In order to merge all Rows into a single batch use DataFrame::collect() method or set size to -1 or 0.
     *
     * @param int<-1, max> $size
     *
     * @lazy
     */
    public function batchSize(int $size): self
    {
        if ($size === -1) {
            return $this->collect();
        }

        if ($size === 0) {
            return $this->collect();
        }

        $this->cursor = new Node\Batch($this->cursor, $size);

        return $this;
    }

    /**
     * Start processing rows up to this moment and put each instance of Rows
     * into previously defined cache.
     * Cache type can be set through ConfigBuilder.
     * By default everything is cached in system tmp dir.
     *
     * Important: cache batch size might significantly improve performance when processing large amount of rows.
     * Larger batch size will increase memory consumption but will reduce number of IO operations.
     * When not set, the batch size is taken from the last DataFrame::batchSize() call.
     *
     * @lazy
     *
     * @param null|string $id
     * @param null|Cache $cache reads of this cache must pass the same instance to from_cache()
     *
     * @throws InvalidArgumentException
     */
    public function cache(?string $id = null, ?int $cacheBatchSize = null, ?Cache $cache = null): self
    {
        if ($cacheBatchSize !== null && $cacheBatchSize < 1) {
            throw new InvalidArgumentException('Cache batch size must be greater than 0');
        }

        $this->cursor = new Node\Cache($this->cursor, $id, $cacheBatchSize, $cache);

        return $this;
    }

    /**
     * Before transforming rows, collect them and merge into single Rows instance.
     * This might lead to memory issues when processing large amount of rows, use with caution.
     *
     * @lazy
     */
    public function collect(): self
    {
        $this->cursor = new Node\Collect($this->cursor);

        return $this;
    }

    /**
     * This method allows to collect references to all entries used in this pipeline.
     *
     * ```php
     * (new Flow())
     *   ->read(From::chain())
     *   ->collectRefs($refs = refs())
     *   ->run();
     * ```
     *
     * @lazy
     */
    public function collectRefs(References $references): self
    {
        $this->cursor = new Node\CollectRefs($this->cursor, $references);

        return $this;
    }

    public function constrain(Constraint $constraint, Constraint ...$constraints): self
    {
        $this->cursor = new Node\Constrain($this->cursor, [$constraint, ...$constraints]);

        return $this;
    }

    /**
     * @trigger
     * Return total count of rows processed by this pipeline.
     */
    public function count(): int
    {
        $total = 0;

        foreach ($this->run->of($this->logical(), $this->context) as $rows) {
            $total += $rows->count();
        }

        return $total;
    }

    /**
     * @lazy
     */
    public function crossJoin(self $dataFrame, string $prefix = ''): self
    {
        $this->cursor = new Node\CrossJoin($this->cursor, new Node\Frame($dataFrame->snapshot()), $prefix);

        return $this;
    }

    /**
     * @param int $limit maximum numbers of rows to display
     * @param bool|int $truncate false or if set to 0 columns are not truncated, otherwise default truncate to 20
     *                           characters
     * @param Formatter $formatter
     *
     * @trigger
     *
     * @throws InvalidArgumentException
     */
    public function display(
        int $limit = 20,
        int|bool $truncate = 20,
        Formatter $formatter = new AsciiTableFormatter(),
    ): string {
        $this->limit($limit);
        $this->collect();

        $output = '';

        foreach ($this->run->of($this->logical(), $this->context) as $rows) {
            $output .= $formatter->format($rows, $truncate);
        }

        return $output;
    }

    /**
     * Drop given entries.
     *
     * @lazy
     */
    public function drop(string|Reference ...$entries): self
    {
        $this->cursor = new Node\Drop($this->cursor, array_values($entries));

        return $this;
    }

    /**
     * @param Reference|string ...$entries
     *
     * @lazy
     *
     * @return $this
     */
    public function dropDuplicates(string|Reference ...$entries): self
    {
        $this->cursor = new Node\Distinct($this->cursor, array_values($entries));

        return $this;
    }

    public function duplicateRow(mixed $condition, WithEntry ...$entries): self
    {
        $this->cursor = new Node\DuplicateRow($this->cursor, $condition, array_values($entries));

        return $this;
    }

    /**
     * Be aware that fetch is not memory safe and will load all rows into memory.
     * If you want to safely iterate over Rows use oe of the following methods:.
     *
     * DataFrame::get() : \Generator
     * DataFrame::getAsArray() : \Generator
     * DataFrame::getEach() : \Generator
     * DataFrame::getEachAsArray() : \Generator
     *
     * @trigger
     *
     * @throws InvalidArgumentException
     */
    public function fetch(?int $limit = null): Rows
    {
        if ($limit !== null) {
            $this->limit($limit);
        }

        $rows = null;
        $plan = $this->run->plan($this->logical(), $this->context);

        foreach ($this->run->execute($plan) as $nextRows) {
            $rows = $rows === null ? $nextRows : $rows->merge($nextRows);
        }

        return $rows ?? new Rows($plan instanceof Plan\Described ? $plan->schema : new Schema());
    }

    /**
     * @lazy
     */
    public function filter(ScalarFunction $function): self
    {
        $this->cursor = new Node\Filter($this->cursor, $function);

        return $this;
    }

    /**
     * @internal engine paths only - a build-time scan has to know whether the source can be read twice
     */
    public function extractor(): Extractor
    {
        return $this->logical()->source()->extractor();
    }

    /**
     * @internal the planner and from_data_frame() take the snapshot a frame embeds
     */
    public function snapshot(): Snapshot
    {
        return new Snapshot($this->logical(), $this->context);
    }

    /**
     * @internal a frame over this frame's cursor and none of its sinks, private to the Sink it is handed to: the
     *           cursor keeps its identity (Sink\Roots attaches by it) and the FlowContext is a copy, so onError()
     *           on the fork never reaches this frame
     */
    public function fork(): self
    {
        $fork = new self(
            $this->logical()->source()->extractor(),
            $this->context->withErrorHandler($this->context->errorHandler()),
        );
        $fork->cursor = $this->cursor;

        return $fork;
    }

    /**
     * @internal
     */
    public function logical(): LogicalPlan
    {
        $result = new Node\Result($this->cursor);

        return new LogicalPlan($this->sinks === [] ? $result : new Node\SinkMultiple($result, ...$this->sinks));
    }

    /**
     * @internal the end of this frame's chain - the node the next verb wraps and a Sink attaches to
     */
    public function cursor(): Node
    {
        return $this->cursor;
    }

    /**
     * @internal
     *
     * @return Sinks
     */
    public function sinks(): array
    {
        return $this->sinks;
    }

    /**
     * @lazy
     *
     * @param array<ScalarFunction> $functions
     */
    public function filters(array $functions): self
    {
        foreach ($functions as $function) {
            $this->filter($function);
        }

        return $this;
    }

    /**
     * @trigger
     *
     * @param null|callable(Rows $rows) : void $callback
     */
    public function forEach(?callable $callback = null): void
    {
        $this->run($callback);
    }

    /**
     * Yields each row as an instance of Rows.
     *
     * @trigger
     *
     * @return \Generator<Rows>
     */
    public function get(): Generator
    {
        foreach ($this->run->of($this->logical(), $this->context) as $rows) {
            yield $rows;
        }
    }

    /**
     * Yields each row as an array.
     *
     * @trigger
     *
     * @return \Generator<array<array<mixed>>>
     */
    public function getAsArray(): Generator
    {
        foreach ($this->run->of($this->logical(), $this->context) as $rows) {
            yield $rows->toArray();
        }
    }

    /**
     * Yield each row as an instance of Row.
     *
     * @trigger
     *
     * @return \Generator<Row>
     */
    public function getEach(): Generator
    {
        foreach ($this->run->of($this->logical(), $this->context) as $rows) {
            foreach ($rows as $row) {
                yield $row;
            }
        }
    }

    /**
     * Yield each row as an array.
     *
     * @trigger
     *
     * @return \Generator<array<mixed>>
     */
    public function getEachAsArray(): Generator
    {
        foreach ($this->run->of($this->logical(), $this->context) as $rows) {
            foreach ($rows as $row) {
                yield $row->toArray();
            }
        }
    }

    /**
     * @lazy
     *
     * @param GroupByReferences|Reference|string $entries a single column is grouped by on its own
     * @param null|GroupByAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function groupBy(
        array|Reference|string $entries,
        ?GroupByAlgorithmBuilder $algorithm = null,
    ): GroupedDataFrame {
        $references = is_array($entries) ? $entries : [$entries];

        return new GroupedDataFrame($this, new GroupBy(...$references), $algorithm);
    }

    /**
     * @lazy
     *
     * @param null|JoinAlgorithmBuilder $algorithm null defers to configuration; a builder pins the algorithm
     *                                            for this operation and skips any automatic choice
     */
    public function join(
        self $dataFrame,
        Expression $on,
        string|Join $type = Join::left,
        ?JoinAlgorithmBuilder $algorithm = null,
    ): self {
        if (is_string($type)) {
            $type = Join::from($type);
        }

        $this->cursor = new Node\Join($this->cursor, new Node\Frame($dataFrame->snapshot()), $on, $type, $algorithm);

        return $this;
    }

    /**
     * Joins in memory per batch; it is not governed by the join algorithm and takes no algorithm override.
     *
     * @lazy
     *
     * @param string|Join $type
     */
    public function joinEach(DataFrameFactory $factory, Expression $on, string|Join $type = Join::left): self
    {
        if (is_string($type)) {
            $type = Join::tryFrom($type) ?? throw new InvalidArgumentException('Unsupported join type');
        }

        $this->cursor = new Node\JoinEach($this->cursor, $factory, $on, $type);

        return $this;
    }

    /**
     * @lazy
     *
     * @throws InvalidArgumentException
     */
    public function limit(?int $limit): self
    {
        if ($limit === null) {
            return $this;
        }

        $this->cursor = new Node\Limit($this->cursor, $limit);

        return $this;
    }

    /**
     * @lazy
     */
    public function load(Loader|Sink $sink): self
    {
        $this->sinks = [...$this->sinks, ...(new Sink\Roots())->of($this, $sink)];

        return $this;
    }

    /**
     * @lazy
     *
     * @param null|SchemaValidator $validator - when null, StrictValidator gets initialized
     */
    public function match(Schema $schema, ?SchemaValidator $validator = null): self
    {
        $this->cursor = new Node\Validate($this->cursor, $schema, $validator ?? new StrictValidator());

        return $this;
    }

    /**
     * Skip given number of rows from the beginning of the dataset.
     * When $offset is null, nothing happens (no rows are skipped).
     *
     * Performance Note: DataFrame must iterate through and process all skipped rows
     * to reach the offset position. For large offsets, this can impact performance
     * as the data source still needs to be read and processed up to the offset point.
     *
     * @param ?int<0, max> $offset
     *
     * @lazy
     *
     * @throws InvalidArgumentException
     */
    public function offset(?int $offset): self
    {
        if ($offset === null) {
            return $this;
        }

        $this->cursor = new Node\Offset($this->cursor, $offset);

        return $this;
    }

    /**
     * @lazy
     */
    public function onError(ErrorHandler $handler): self
    {
        $this->context->setErrorHandler($handler);

        return $this;
    }

    /**
     * @lazy
     * Shuffles the stream so every row sharing the given columns arrives in one batch. It does not
     * write directories - that is declared on the loader, `to_csv(...)->partitionBy('region')`.
     */
    public function repartition(string|Reference $entry, string|Reference ...$entries): self
    {
        array_unshift($entries, $entry);

        $this->cursor = new Node\Repartition($this->cursor, References::init(...$entries));

        return $this;
    }

    /**
     * @trigger
     */
    public function printRows(
        ?int $limit = 20,
        int|bool $truncate = 20,
        Formatter $formatter = new AsciiTableFormatter(),
    ): void {
        if ($limit !== null) {
            $this->limit($limit);
        }

        $this->collect();
        $this->load(to_output($truncate, Output::rows, $formatter));

        $this->run();
    }

    /**
     * The logical plan as a tree: one line per node with its declarations, a subtree several consumers share
     * printed once. Answers from the plan without reading a row.
     */
    public function explain(): string
    {
        return (new Plan\Explain())->of($this->logical());
    }

    /**
     * @lazy
     *
     * @throws SchemaNotDerivableException
     */
    public function printSchema(SchemaFormatter $formatter = new ASCIISchemaFormatter()): void
    {
        echo $formatter->format($this->schema());
    }

    /**
     * @internal engine paths only - GroupedDataFrame builds its steps against this frame's plan
     */
    public function registerGroupBy(GroupBy $groupBy, ?GroupByAlgorithmBuilder $algorithm = null): void
    {
        $this->cursor = new Node\Aggregate($this->cursor, $groupBy, $algorithm);
    }

    /**
     * @lazy
     */
    public function rename(string $from, string $to): self
    {
        $this->cursor = new Node\Rename($this->cursor, $from, $to);

        return $this;
    }

    public function renameEach(RenameEntryStrategy ...$strategies): self
    {
        $this->cursor = new Node\RenameEach($this->cursor, array_values($strategies));

        return $this;
    }

    /**
     * @lazy
     * Alias for ETL::transform method.
     */
    public function rows(Transformer|Transformation $transformer): self
    {
        return $this->with($transformer);
    }

    /**
     * @trigger
     *
     * When analyzing pipeline execution we can chose to collect various metrics through analyze()->with*() method
     *
     * - column statistics - analyze()->withColumnStatistics()
     * - schema - analyze()->withSchema()
     *
     * @param null|callable(Rows $rows, FlowContext $context): void $callback
     * @param Analyze|bool $analyze - when set run will return Report
     *
     * @return ($analyze is Analyze|true ? Report : null)
     */
    public function run(?callable $callback = null, bool|Analyze $analyze = false): ?Report
    {
        if ($analyze === false) {
            $analyze = $this->context->config->analyze();
        }

        $collector = new StatisticsCollector($analyze, $this->context);

        foreach ($this->run->of($this->logical(), $this->context) as $rows) {
            if ($callback !== null) {
                $callback($rows, $this->context);
            }

            $collector->capture($rows);
        }

        return $collector->report();
    }

    /**
     * @lazy
     *
     * @throws SchemaNotDerivableException
     */
    public function schema(): Schema
    {
        $plan = $this->run->plan($this->logical(), $this->context);

        return $plan instanceof Plan\Described ? $plan->schema : throw $plan->why->toException();
    }

    /**
     * @lazy
     * Keep only given entries.
     */
    public function select(string|Reference ...$entries): self
    {
        $this->cursor = new Node\Select($this->cursor, array_values($entries));

        return $this;
    }

    /**
     * @lazy
     *
     * @param Reference|SortReferences|string $entries a single column is sorted by on its own
     * @param null|SortAlgorithmBuilder $algorithm null defers to configuration; a builder pins the algorithm
     *                                            for this operation and skips any automatic choice
     */
    public function sortBy(array|Reference|string $entries, ?SortAlgorithmBuilder $algorithm = null): self
    {
        $references = is_array($entries) ? $entries : [$entries];

        $this->cursor = new Node\Sort($this->cursor, refs(...$references), $algorithm);

        return $this;
    }

    /**
     * Alias for DataFrame::with().
     *
     * @lazy
     */
    public function transform(Transformer|Transformation|Transformations|WithEntry $transformer): self
    {
        return $this->with($transformer);
    }

    /**
     * The difference between filter and until is that filter will keep filtering rows until extractors finish yielding
     * rows. Until will send a STOP signal to the Extractor when the condition is not met.
     *
     * @lazy
     */
    public function until(ScalarFunction $function): self
    {
        $this->cursor = new Node\Until($this->cursor, $function);

        return $this;
    }

    /**
     * @lazy
     * This method is useful mostly in development when
     * you want to pause processing at certain moment without
     * removing code. All operations will get processed up to this point,
     * from here no rows are passed forward.
     */
    public function void(): self
    {
        $this->cursor = new Node\Discard($this->cursor);

        return $this;
    }

    /**
     * @lazy
     */
    public function with(Transformer|Transformation|Transformations|WithEntry $transformer): self
    {
        if ($transformer instanceof Transformer) {
            $this->cursor = new Node\Transform($this->cursor, $transformer);

            return $this;
        }

        if ($transformer instanceof Transformations) {
            $transformer->transform($this);

            return $this;
        }

        if ($transformer instanceof WithEntry) {
            $this->withEntry($transformer->name, $transformer->function);

            return $this;
        }

        return $transformer->transform($this);
    }

    /**
     * @lazy
     *
     * @param array<int, WithEntry>|array<string, ScalarFunction|WindowFunction|WithEntry> $references
     */
    public function withEntries(array $references): self
    {
        foreach ($references as $entryName => $ref) {
            if ($ref instanceof WithEntry) {
                $this->withEntry($ref->name, $ref->function);
            } else {
                $this->withEntry((string) $entryName, $ref);
            }
        }

        return $this;
    }

    /**
     * @param Definition<mixed>|string $entry
     *
     * @lazy
     */
    public function withEntry(string|Definition $entry, ScalarFunction|WindowFunction $reference): self
    {
        $this->cursor = $reference instanceof WindowFunction
            ? new Node\WindowColumn($this->cursor, $entry, $reference)
            : new Node\WithColumn($this->cursor, $entry, $reference);

        return $this;
    }

    /**
     * @lazy
     * Alias for ETL::load function.
     */
    public function write(Loader|Sink $sink): self
    {
        return $this->load($sink);
    }
}

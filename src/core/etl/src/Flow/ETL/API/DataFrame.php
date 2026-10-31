<?php

declare(strict_types=1);

namespace Flow\ETL\API;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Functions\ExpandingFunctions;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Functions\WithColumn;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\ExtractorFactory;
use Flow\ETL\IO\Formatter;
use Flow\ETL\IO\Formatter\AsciiTableFormatter;
use Flow\ETL\IO\Formatter\Row\ASCIISchemaFormatter;
use Flow\ETL\IO\Formatter\Schema\SchemaFormatter;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\StreamLoader;
use Flow\ETL\IO\Loader\StreamLoader\Output;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Operators\Constraint;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Operators\Transformer\Rename\RenameColumnStrategy;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\BucketingAlgorithmBuilder;
use Flow\ETL\Planning\Logical\Node\JoinAlgorithmBuilder;
use Flow\ETL\Planning\Logical\Node\SortAlgorithmBuilder;
use Flow\ETL\Planning\Logical\Plan;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Runtime\Analyze;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Runtime\ErrorHandler;
use Flow\ETL\Runtime\Executor\StatisticsCollector;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Runtime\Metrics\Report;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Validator\StrictValidator;
use Flow\ETL\Values\SchemaValidator;
use Generator;

use function array_map;
use function array_unique;
use function array_unshift;
use function array_values;
use function count;
use function Flow\Types\DSL\type_integer;
use function is_array;
use function is_string;

/**
 * @type Aggregations      = list<AggregatingFunction>
 * @type GroupByReferences = list<string|Reference>
 * @type SortColumns       = list<string|Reference|SortColumn>
 */
final class DataFrame
{
    private FlowContext $context;

    private readonly Extractor $extractor;

    private Node $root;

    private Writes $writes;

    public function __construct(Extractor $extractor, Config|FlowContext $context)
    {
        $this->context = $context instanceof FlowContext ? $context : new FlowContext($context);
        $this->extractor = $extractor;
        $this->root = new Node\Read($extractor);
        $this->writes = new Writes();
    }

    /**
     * @lazy
     *
     * @param Aggregations $aggregations
     * @param null|BucketingAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function aggregate(array $aggregations, ?BucketingAlgorithmBuilder $algorithm = null): self
    {
        if ($aggregations === []) {
            throw new InvalidArgumentException("Aggregations can't be empty");
        }

        return $this->aggregateBy(new GroupBy(new References(), new Aggregators(...$aggregations)), $algorithm);
    }

    /**
     * @lazy
     *
     * @param null|BucketingAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function aggregateBy(GroupBy $groupBy, ?BucketingAlgorithmBuilder $algorithm = null): self
    {
        $this->root = new Node\Aggregate($this->root, $groupBy, $algorithm);

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
        $this->root = new Node\BatchBy($this->root, UnresolvedReference::init($column), $minSize);

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

        $this->root = new Node\Batch($this->root, new BatchSize($size));

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
     * @param null|RowsStorage $cache reads of this cache must pass the same instance to from_cache()
     *
     * @throws InvalidArgumentException
     */
    public function cache(?string $id = null, ?int $cacheBatchSize = null, ?RowsStorage $cache = null): self
    {
        if ($cacheBatchSize !== null && $cacheBatchSize < 1) {
            throw new InvalidArgumentException('Cache batch size must be greater than 0');
        }

        $this->root = new Node\Cache($this->root, $id, $cacheBatchSize, $cache);

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
        $this->root = new Node\Collect($this->root);

        return $this;
    }

    /**
     * This method allows to collect references to all columns used in this pipeline.
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
        $this->root = new Node\CollectRefs($this->root, $references);

        return $this;
    }

    public function constrain(Constraint $constraint, Constraint ...$constraints): self
    {
        $this->root = new Node\Constrain($this->root, [$constraint, ...$constraints]);

        return $this;
    }

    /**
     * @trigger
     * Return total count of rows processed by this pipeline.
     */
    public function count(): int
    {
        return type_integer()->assert(
            $this->context
                ->config
                ->executor()
                ->fetch(Trigger::count->plan($this->root, $this->writes), $this->context)
                ->column('count')
                ->value(0),
        );
    }

    /**
     * @lazy
     */
    public function crossJoin(self $dataFrame, string $prefix = ''): self
    {
        $this->root = new Node\CrossJoin($this->root, $dataFrame->explain(Trigger::rows)->logical->root, $prefix);

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

        foreach ($this->context
            ->config
            ->executor()
            ->execute(Trigger::rows->plan($this->root, $this->writes), $this->context) as $rows) {
            $output .= $formatter->format($rows, $truncate);
        }

        return $output;
    }

    /**
     * Drop given columns.
     *
     * @lazy
     */
    public function drop(string|Reference ...$columns): self
    {
        $this->root = new Node\Drop($this->root, array_values($columns));

        return $this;
    }

    /**
     * @param Reference|string ...$columns
     *
     * @lazy
     *
     * @return $this
     */
    public function dropDuplicates(string|Reference ...$columns): self
    {
        $this->root = new Node\Distinct($this->root, array_values($columns));

        return $this;
    }

    public function duplicateRow(mixed $condition, WithColumn ...$columns): self
    {
        $this->root = new Node\DuplicateRow($this->root, $condition, array_values($columns));

        return $this;
    }

    /**
     * Be aware that fetch is not memory safe and will load all rows into memory.
     * If you want to safely iterate over Rows use oe of the following methods:.
     *
     * DataFrame::get() : \Generator
     * DataFrame::getAsArray() : \Generator
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

        return $this->context
            ->config
            ->executor()
            ->fetch(Trigger::rows->plan($this->root, $this->writes), $this->context);
    }

    /**
     * @lazy
     */
    public function filter(ScalarFunction $function): self
    {
        $this->root = new Node\Filter($this->root, $function);

        return $this;
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
     * @param null|callable(Rows $rows, FlowContext $context) : void $callback
     */
    public function forEach(?callable $callback = null): void
    {
        foreach ($this->context
            ->config
            ->executor()
            ->execute(Trigger::rows->plan($this->root, $this->writes), $this->context) as $rows) {
            if ($callback !== null) {
                $callback($rows, $this->context);
            }
        }
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
        foreach ($this->context
            ->config
            ->executor()
            ->execute(Trigger::rows->plan($this->root, $this->writes), $this->context) as $rows) {
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
        foreach ($this->context
            ->config
            ->executor()
            ->execute(Trigger::rows->plan($this->root, $this->writes), $this->context) as $rows) {
            yield $rows->toArray();
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
        foreach ($this->context
            ->config
            ->executor()
            ->execute(Trigger::rows->plan($this->root, $this->writes), $this->context) as $rows) {
            foreach ($rows->toArray() as $row) {
                yield $row;
            }
        }
    }

    /**
     * @lazy
     *
     * @param GroupByReferences|Reference|string $columns a single column is grouped by on its own
     * @param null|BucketingAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function groupBy(
        array|Reference|string $columns,
        ?BucketingAlgorithmBuilder $algorithm = null,
    ): GroupedDataFrame {
        // the rows feeding the aggregate, without the frame's writes - what pivot discovery scans
        $input = new self($this->extractor, $this->context);
        $input->root = $this->root;

        return new GroupedDataFrame(
            $this,
            $input,
            References::init(...array_unique(is_array($columns) ? $columns : [$columns])),
            $algorithm,
        );
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
        string|JoinType $type = JoinType::left,
        ?JoinAlgorithmBuilder $algorithm = null,
    ): self {
        if (is_string($type)) {
            $type = JoinType::from($type);
        }

        $this->root = new Node\Join(
            $this->root,
            $dataFrame->explain(Trigger::rows)->logical->root,
            $on,
            $type,
            $algorithm,
        );

        return $this;
    }

    /**
     * Joins in memory per batch; it is not governed by the join algorithm and takes no algorithm override.
     *
     * @lazy
     *
     * @param string|JoinType $type
     */
    public function joinEach(ExtractorFactory $factory, Expression $on, string|JoinType $type = JoinType::left): self
    {
        if (is_string($type)) {
            $type = JoinType::tryFrom($type) ?? throw new InvalidArgumentException('Unsupported join type');
        }

        $this->root = new Node\JoinEach($this->root, $factory, $on, $type);

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

        $this->root = new Node\Limit($this->root, $limit);

        return $this;
    }

    /**
     * @lazy
     *
     * @param null|SchemaValidator $validator - when null, StrictValidator gets initialized
     */
    public function match(Schema $schema, ?SchemaValidator $validator = null): self
    {
        $this->root = new Node\Validate($this->root, $schema, $validator ?? new StrictValidator());

        return $this;
    }

    /**
     * Skip given number of rows from the beginning of the dataset.
     * When $offset is null, nothing happens (no rows are skipped).
     *
     * Performance Note: directly over a file source, the source skips the rows itself (Parquet and Floe by their
     * footer, without decoding them). Any other source, or an offset after a step that drops or reorders rows, still
     * reads and processes every skipped row.
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

        $this->root = new Node\Offset($this->root, $offset);

        return $this;
    }

    /**
     * @lazy
     */
    public function onError(ErrorHandler $handler): self
    {
        $this->context = $this->context->withErrorHandler($handler);

        return $this;
    }

    /**
     * @lazy
     * Shuffles the stream so every row sharing the given columns arrives in one batch. It does not
     * write directories - that is declared on the loader, `to_csv(...)->partitionBy('region')`.
     */
    public function repartition(string|Reference $column, string|Reference ...$columns): self
    {
        array_unshift($columns, $column);

        $this->root = new Node\Repartition($this->root, References::init(...$columns));

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
        $this->write(StreamLoader::output($truncate, Output::rows, $formatter));

        $this->run();
    }

    /**
     * The plan $trigger would run over this frame, frozen: later verbs on this frame do not reach it. The default
     * adds no consumer of its own - it draws the frame as built, its chain and its writes. toString() prints it as a
     * tree. Answers from the plan without reading a row.
     */
    public function explain(Trigger $trigger = Trigger::run): Plan
    {
        return Plan::of($trigger->plan($this->root, $this->writes), $this->context->config->planner());
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
     * @lazy
     */
    public function rename(string $from, string $to): self
    {
        $this->root = new Node\Rename($this->root, $from, $to);

        return $this;
    }

    public function renameEach(RenameColumnStrategy ...$strategies): self
    {
        $this->root = new Node\RenameEach($this->root, array_values($strategies));

        return $this;
    }

    /**
     * @trigger
     *
     * When analyzing pipeline execution we can chose to collect various metrics through analyze()->with*() method
     *
     * - column statistics - analyze()->withColumnStatistics()
     * - schema - analyze()->withSchema()
     *
     * @param Analyze|bool $analyze - when set run will return Report
     *
     * @return ($analyze is Analyze|true ? Report : null)
     */
    public function run(bool|Analyze $analyze = false): ?Report
    {
        if ($analyze === false) {
            $analyze = $this->context->config->analyze();
        }

        $collector = new StatisticsCollector($analyze, $this->context);

        foreach ($this->context
            ->config
            ->executor()
            ->execute(
                ($collector->readsColumns() ? Trigger::rows : Trigger::run)->plan($this->root, $this->writes),
                $this->context,
                $collector->counts(),
            ) as $rows) {
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
        return $this->context->config->planner()->plan(Trigger::rows->plan($this->root, $this->writes))->schema();
    }

    /**
     * @lazy
     * Keep only given columns.
     */
    public function select(string|Reference ...$columns): self
    {
        $this->root = new Node\Select($this->root, array_values($columns));

        return $this;
    }

    /**
     * @lazy
     *
     * @param Reference|SortColumn|SortColumns|string $columns a single column is sorted by on its own; a reference sorts ascending
     * @param null|SortAlgorithmBuilder $algorithm null defers to configuration; a builder pins the algorithm
     *                                            for this operation and skips any automatic choice
     */
    public function sortBy(array|Reference|SortColumn|string $columns, ?SortAlgorithmBuilder $algorithm = null): self
    {
        $this->root = new Node\Sort(
            $this->root,
            array_map(
                static fn(string|Reference|SortColumn $column): SortColumn => $column instanceof SortColumn
                    ? $column
                    : UnresolvedReference::init($column)->asc(),
                is_array($columns) ? array_values($columns) : [$columns],
            ),
            $algorithm,
        );

        return $this;
    }

    /**
     * The difference between filter and until is that filter will keep filtering rows until extractors finish yielding
     * rows. Until will send a STOP signal to the Extractor when the condition is not met.
     *
     * @lazy
     */
    public function until(ScalarFunction $function): self
    {
        $this->root = new Node\Until($this->root, $function);

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
        $this->root = new Node\Discard($this->root);

        return $this;
    }

    /**
     * @lazy
     */
    public function with(Transformer|Transformation|WithColumn $transformer): self
    {
        if ($transformer instanceof Transformer) {
            $this->root = new Node\Transform($this->root, $transformer);

            return $this;
        }

        if ($transformer instanceof WithColumn) {
            $this->withColumn($transformer->name, $transformer->function);

            return $this;
        }

        return $transformer->transform($this);
    }

    /**
     * @param Definition<mixed>|string $column
     *
     * @lazy
     */
    public function withColumn(string|Definition $column, ScalarFunction|WindowFunction $reference): self
    {
        $this->root = match (true) {
            $reference instanceof WindowFunction => new Node\WindowColumn($this->root, $column, $reference),
            (new ExpandingFunctions())->in($reference) !== [] => new Node\ExpandColumn(
                $this->root,
                $column,
                $reference,
            ),
            default => new Node\WithColumn($this->root, $column, $reference),
        };

        return $this;
    }

    /**
     * @lazy
     *
     * @param array<int, WithColumn>|array<string, ScalarFunction|WindowFunction|WithColumn> $references
     */
    public function withColumns(array $references): self
    {
        foreach ($references as $name => $ref) {
            if ($ref instanceof WithColumn) {
                $this->withColumn($ref->name, $ref->function);
            } else {
                $this->withColumn((string) $name, $ref);
            }
        }

        return $this;
    }

    /**
     * @lazy
     */
    public function write(Loader|Branch|Transactional $write): self
    {
        if ($write instanceof Loader) {
            $this->writes = $this->writes->merge(new Writes(new Node\Write($this->root, $write)));

            return $this;
        }

        $prefix = new self($this->extractor, $this->context);
        $prefix->root = $this->root;

        if ($write instanceof Transactional) {
            foreach ($write->writes() as $child) {
                $prefix->write($child);
            }

            $writes = [];

            foreach ($prefix->writes as $root) {
                // checked BEFORE the splat below, or PHP raises a TypeError first
                $writes[] = $root instanceof Node\Transaction
                    ? throw InvalidLogicException::nestedTransaction()
                    : $root;
            }

            $this->writes = $this->writes->merge(new Writes(new Node\Transaction($write->transaction(), ...$writes)));

            return $this;
        }

        $write->write($prefix);

        if ($prefix->context->errorHandler() !== $this->context->errorHandler()) {
            throw InvalidLogicException::errorHandlerInsideBranch($write::class);
        }

        $this->writes = $this->writes->merge($prefix->writes);

        return $this;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Kernels\Sort\ExternalSort;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\BoundedRead;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Sort\SortColumn;
use Generator;
use SensitiveParameter;

use function array_values;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function max;

/**
 * Holds at most 2×$limit rows plus one batch; ties may order unlike ExternalSort.
 */
final readonly class TopNProcessor implements Distributable
{
    public ?ExternalSort $fallback;

    /**
     * @param ExternalSortConfig|MemorySortConfig $config the effective sort config: the operation's own or the frame's;
     *                                                    an external one spills past its memory limit
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        /** @var list<SortColumn> */
        public array $columns,
        public int $limit,
        public MemorySortConfig|ExternalSortConfig $config,
        private RandomValueGenerator $random,
        private ?Schema $declared = null,
        private Progress $progress = new VoidProgress(),
    ) {
        $this->fallback = $config instanceof ExternalSortConfig
            ? ExternalSort::of($columns, $config, $random, $progress)
            : null;

        if ($this->limit < 1) {
            throw new InvalidArgumentException('TopN limit must be greater than 0, given: ' . $this->limit);
        }
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $config = $rebuild->of($data['config'] ?? null, Describable::class);

        return new self(
            array_values($rebuild->all($data['columns'] ?? null, SortColumn::class)),
            type_integer()->assert($data['limit'] ?? null),
            $config instanceof MemorySortConfig || $config instanceof ExternalSortConfig
                ? $config
                : throw InvalidArgumentException::because('A top-N config is a memory or an external sort config'),
            type_instance_of(RandomValueGenerator::class)->assert($rebuild->of(
                $data['random'] ?? null,
                Describable::class,
            )),
            ($data['declared'] ?? null) === null ? null : Schema::fromArray(type_array()->assert($data['declared'])),
            $rebuild->progress(StageName::sort, $data['progress'] ?? null),
        );
    }

    /**
     * null when the random value generator or the sort config does not describe.
     */
    public function describe(): ?Description
    {
        return (
            $this->random instanceof Describable
                ? Description::compose(self::class, [
                    'columns' => $this->columns,
                    'limit' => $this->limit,
                    'config' => $this->config,
                    'random' => $this->random,
                    'declared' => $this->declared?->normalize(),
                    'progress' => $this->progress instanceof Describable ? $this->progress : new VoidProgress(),
                ])
                : null
        );
    }

    /**
     * Every split keeps its own top rows; one process keeps the top of those. null when it does not describe.
     */
    public function distribution(): ?Distribution
    {
        return $this->describe() === null ? null : Distribution::single($this, progress: $this->progress);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(
            new self($this->columns, $this->limit, $this->config, $this->random, $input, $this->progress),
            $input,
        );
    }

    public function breaks(): bool
    {
        return true;
    }

    public function process(Generator $rows, Backend $backend): Generator
    {
        $kept = null;
        $maxSize = 1;
        $schema = null;
        // pulled before the budget starts: what a lazy upstream allocates on its first pull (a join build) is not ours
        $rows->valid();
        $budget = $this->config instanceof ExternalSortConfig
            ? MemoryBudget::of($backend, $this->config->bucketing->memoryLimit)
            : null;

        while ($rows->valid()) {
            $batch = $rows->current();
            $rows->next();
            $schema ??= $batch->schema();

            if ($batch->isEmpty()) {
                continue;
            }

            $maxSize = max($batch->count(), $maxSize);
            $this->progress->advanced($batch->count());
            $conformed = $batch->matchTo($this->declared ?? $schema, $backend);
            $kept = $kept === null ? $conformed : Rows::concat($kept->schema(), $backend, $kept, $conformed);

            // trimming only past twice the limit keeps the re-sorts amortised
            if ($kept->count() > (2 * $this->limit)) {
                $kept = $this->top($kept, $backend);
            }

            // past the memory limit the kept rows and the rest of the stream go to the spilling sort; a trimmed row
            // cannot be in the top $limit, so the result is the same
            if ($this->fallback !== null && $budget !== null && $budget->exceeded()) {
                // the kept rows were counted when top-n took them, the rest is counted as the sort takes it
                yield from $this->fallback->sort(
                    BoundedRead::followedBy([$kept], $this->fallback->counted($rows)),
                    $backend,
                    $budget,
                    $this->limit,
                );
                $this->progress->done();

                return;
            }
        }

        if ($kept === null) {
            yield from Rows::empty($this->declared ?? $schema ?? new Schema(), $backend)->chunks($maxSize);
            $this->progress->done();

            return;
        }

        foreach ($this->top($kept, $backend)->chunks($maxSize) as $chunk) {
            $this->progress->busy();

            yield $chunk;
        }

        $this->progress->done();
    }

    public function top(Rows $rows, Backend $backend): Rows
    {
        return $backend->sort($rows, $this->columns)->take($this->limit);
    }
}

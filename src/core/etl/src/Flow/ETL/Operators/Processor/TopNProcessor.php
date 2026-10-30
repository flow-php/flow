<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Kernels\Sort\ExternalSort;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Kernels\Sort\RowOrder;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Memory\BoundedRead;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function array_values;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function max;

/**
 * Keeps the first $limit rows by $columns while it reads, so it never holds more than twice $limit rows plus one
 * batch. Sorts the same way MemorySortProcessor does: it emits the rows a sort followed by a limit would, though rows
 * with equal keys may come in another order than an external sort leaves them.
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
    ) {
        $this->fallback = $config instanceof ExternalSortConfig ? ExternalSort::of($columns, $config, $random) : null;

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
                ])
                : null
        );
    }

    /**
     * Every split keeps its own top rows; one process keeps the top of those. null when it does not describe.
     */
    public function distribution(): ?Distribution
    {
        return $this->describe() === null ? null : Distribution::single($this);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->columns, $this->limit, $this->config, $this->random, $input), $input);
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
        $budget = $this->fallback === null ? null : new MemoryBudget($backend, $this->fallback->memoryLimit);

        while ($rows->valid()) {
            $batch = $rows->current();
            $rows->next();
            $schema ??= $batch->schema();

            if ($batch->isEmpty()) {
                continue;
            }

            $maxSize = max($batch->count(), $maxSize);
            $conformed = $batch->matchTo($this->declared ?? $schema, $backend);
            $kept = $kept === null ? $conformed : Rows::concat($kept->schema(), $backend, $kept, $conformed);

            // trimming only past twice the limit keeps the re-sorts amortised
            if ($kept->count() > (2 * $this->limit)) {
                $kept = $this->top($kept);
            }

            // past the memory limit the kept rows and the rest of the stream go to the spilling sort; a trimmed row
            // cannot be in the top $limit, so the result is the same
            if ($this->fallback !== null && $budget?->exceeded()) {
                yield from $this->fallback->sort(BoundedRead::followedBy([$kept], $rows), $backend, $this->limit);

                return;
            }
        }

        if ($kept === null) {
            yield from Rows::empty($this->declared ?? $schema ?? new Schema(), $backend)->chunks($maxSize);

            return;
        }

        yield from $this->top($kept)->chunks($maxSize);
    }

    public function top(Rows $rows): Rows
    {
        $order = new RowOrder($this->columns);

        return $rows->gather($order->permutation($order->keys($rows), $rows->count()))->take($this->limit);
    }
}

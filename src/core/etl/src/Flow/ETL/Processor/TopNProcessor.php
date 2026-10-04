<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\Dataset\Memory\MemoryBudget;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Sort\ExternalSort;
use Generator;

use function max;

/**
 * Keeps the first $limit rows by $refs while it reads, so it never holds more than twice $limit rows plus one
 * batch. Sorts the same way MemorySortProcessor does: it emits the rows a sort followed by a limit would, though rows
 * with equal keys may come in another order than an external sort leaves them.
 */
final readonly class TopNProcessor implements Processor
{
    /**
     * @throws InvalidArgumentException
     */
    public function __construct(
        public References $refs,
        public int $limit,
        public ?ExternalSort $fallback = null,
        private ?Schema $declared = null,
    ) {
        if ($this->limit < 1) {
            throw new InvalidArgumentException('TopN limit must be greater than 0, given: ' . $this->limit);
        }
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->refs, $this->limit, $this->fallback, $input), $input);
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $kept = null;
        $maxSize = 1;
        $schema = null;
        $budget = $this->fallback === null ? null : new MemoryBudget($context->backend(), $this->fallback->memoryLimit);

        while ($rows->valid()) {
            $batch = $rows->current();
            $rows->next();
            $schema ??= $batch->schema();

            if ($batch->isEmpty()) {
                continue;
            }

            $maxSize = max($batch->count(), $maxSize);
            $conformed = $batch->matchTo($this->declared ?? $schema, $context->backend());
            $kept = $kept === null ? $conformed : $kept->concat($context->backend(), $conformed);

            // trimming only past twice the limit keeps the re-sorts amortised
            if ($kept->count() > (2 * $this->limit)) {
                $kept = $this->top($kept);
            }

            // past the memory limit the kept rows and the rest of the stream go to the spilling sort; a trimmed row
            // cannot be in the top $limit, so the result is the same
            if ($this->fallback !== null && $budget?->exceeded()) {
                yield from $this->fallback->sort($this->followedBy($kept, $rows), $context, $this->limit);

                return;
            }
        }

        if ($kept === null) {
            yield from Rows::empty($this->declared ?? $schema ?? new Schema(), $context->backend())->chunks($maxSize);

            return;
        }

        yield from $this->top($kept)->chunks($maxSize);
    }

    /**
     * @param Generator<Rows> $rest
     *
     * @return Generator<Rows>
     */
    public function followedBy(Rows $kept, Generator $rest): Generator
    {
        yield $kept;

        // continues the half-read stream where it stopped; PHP refuses to yield from a generator that already finished
        if ($rest->valid()) {
            yield from $rest;
        }
    }

    public function top(Rows $rows): Rows
    {
        return $rows->sortBy(...$this->refs->all())->take($this->limit);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function count;
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
        private References $refs,
        private int $limit,
        private ?Schema $declared = null,
    ) {
        if ($this->limit < 1) {
            throw new InvalidArgumentException('TopN limit must be greater than 0, given: ' . $this->limit);
        }
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->refs, $this->limit, $input), $input);
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        /** @var array<Row> $kept */
        $kept = [];
        $maxSize = 1;
        $schema = null;

        foreach ($rows as $batch) {
            $schema ??= $batch->schema();

            if ($batch->empty()) {
                continue;
            }

            $maxSize = max($batch->count(), $maxSize);

            foreach ($batch->all() as $row) {
                $kept[] = $row;
            }

            // trimming only past twice the limit keeps the re-sorts amortised
            if (count($kept) > (2 * $this->limit)) {
                $kept = $this->top($kept, $schema);
            }
        }

        yield from (new Rows($this->declared ?? $schema ?? new Schema(), ...$this->top($kept, $schema)))->chunks(
            $maxSize,
        );
    }

    /**
     * @param array<Row> $rows
     *
     * @return array<Row>
     */
    public function top(array $rows, ?Schema $schema): array
    {
        return (new Rows($this->declared ?? $schema ?? new Schema(), ...$rows))
            ->sortBy(...$this->refs->all())
            ->take($this->limit)
            ->all();
    }
}

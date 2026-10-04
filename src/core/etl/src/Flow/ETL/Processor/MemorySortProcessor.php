<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_slice;
use function max;

/**
 * Buffers all rows and sorts them in memory. Registered by SortSteps when the sort algorithm is
 * memory_sort().
 */
final readonly class MemorySortProcessor implements Processor
{
    public function __construct(
        public References $refs,
        private ?Schema $declared = null,
    ) {}

    public function process(Generator $rows, FlowContext $context): Generator
    {
        /** @var list<Rows> $buffer */
        $buffer = [];
        $maxSize = 1;

        $schema = null;

        foreach ($rows as $batch) {
            $schema ??= $batch->schema();
            if ($batch->isEmpty()) {
                continue;
            }

            $maxSize = max($batch->count(), $maxSize);
            $buffer[] = $batch->matchTo($this->declared ?? $schema, $context->backend());
        }

        $all = $buffer === []
            ? Rows::empty($this->declared ?? $schema ?? new Schema(), $context->backend())
            : $buffer[0]->concat($context->backend(), ...array_slice($buffer, 1));

        yield from $all->sortBy(...$this->refs->all())->chunks($maxSize);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->refs, $input), $input);
    }
}

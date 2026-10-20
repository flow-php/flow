<?php

declare(strict_types=1);

namespace Flow\Parquet\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Evaluation\FilterEvaluator;
use Generator;

use function array_fill_keys;
use function array_flip;
use function array_intersect_key;
use function array_keys;
use function array_slice;
use function array_values;
use function count;
use function in_array;
use function min;

/**
 * The matching rows of a read's batches (every row without an evaluator), offset and limit counted over them,
 * rechunked to the batch size; it stops pulling batches at the limit.
 */
final readonly class FilteredBatches
{
    /**
     * @param list<string> $columns the columns returned
     * @param int<1, max> $batchSize
     */
    public function __construct(
        private ?FilterEvaluator $evaluator,
        private array $columns,
        private int $offset = 0,
        private ?int $limit = null,
        private int $batchSize = 1024,
    ) {
        if ($columns === []) {
            throw new InvalidArgumentException('A filtered read returns at least one column');
        }
    }

    /**
     * The columns each batch must hold: the returned ones and the filter's root columns.
     *
     * @return list<string>
     */
    public function columns(): array
    {
        $columns = $this->columns;

        foreach ($this->evaluator?->roots() ?? [] as $root) {
            if (!in_array($root, $columns, true)) {
                $columns[] = $root;
            }
        }

        return $columns;
    }

    /**
     * @param iterable<array<string, list<mixed>>> $batches
     *
     * @return Generator<int, array<string, list<mixed>>>
     */
    public function filter(iterable $batches): Generator
    {
        if ($this->limit === 0) {
            return;
        }

        $skip = $this->offset;
        $remaining = $this->limit;
        $buffer = array_fill_keys($this->columns, []);
        $buffered = 0;

        foreach ($batches as $batch) {
            $mask = $this->evaluator?->mask($batch) ?? array_keys($batch[$this->columns[0]]);

            if ($skip > 0) {
                $skipped = min($skip, count($mask));
                $mask = array_slice($mask, $skipped);
                $skip -= $skipped;
            }

            if ($remaining !== null) {
                $mask = array_slice($mask, 0, $remaining);
                $remaining -= count($mask);
            }

            $whole = count($mask) === count($batch[$this->columns[0]]);
            $rows = $whole ? [] : array_flip($mask);

            foreach ($this->columns as $column) {
                $selected = $whole ? $batch[$column] : array_values(array_intersect_key($batch[$column], $rows));
                $buffer[$column] = $buffer[$column] === [] ? $selected : [...$buffer[$column], ...$selected];
            }

            $buffered += count($mask);

            while ($buffered >= $this->batchSize) {
                $chunk = [];

                foreach ($this->columns as $column) {
                    $chunk[$column] = array_slice($buffer[$column], 0, $this->batchSize);
                    $buffer[$column] = array_slice($buffer[$column], $this->batchSize);
                }

                $buffered -= $this->batchSize;

                yield $chunk;
            }

            if ($remaining === 0) {
                break;
            }
        }

        if ($buffered > 0) {
            yield $buffer;
        }
    }
}

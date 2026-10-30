<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use Flow\ETL\Values\Rows;
use Generator;

use function array_diff_key;
use function array_keys;
use function range;

/**
 * Drops the rows whose join key holds a null: they can never match, and their unmatched padding never reaches the
 * output for the join type the filter is used with.
 */
final readonly class NullKeyFilter
{
    /**
     * @param list<string> $columns
     */
    public function __construct(
        private array $columns,
    ) {}

    public function of(Rows $batch): Rows
    {
        if ($batch->isEmpty()) {
            return $batch;
        }

        $null = [];

        foreach ($this->columns as $name) {
            $column = $batch->column($name);

            if ($column->nullCount() === 0) {
                continue;
            }

            for ($i = 0, $count = $column->count(); $i < $count; $i++) {
                if ($column->isNull($i)) {
                    $null[$i] = true;
                }
            }
        }

        return $null === [] ? $batch : $batch->gather(array_keys(array_diff_key(range(0, $batch->count() - 1), $null)));
    }

    /**
     * A value sent in is forwarded to $rows, and the filter returns.
     *
     * @template TSend
     *
     * @param Generator<mixed, Rows, null|TSend, mixed> $rows
     *
     * @return Generator<int, Rows, null|TSend, void>
     */
    public function rows(Generator $rows): Generator
    {
        foreach ($rows as $batch) {
            $batch = $this->of($batch);

            if ($batch->isEmpty()) {
                continue;
            }

            $sent = yield $batch;

            if ($sent !== null) {
                $rows->send($sent);

                return;
            }
        }
    }
}

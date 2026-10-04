<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Memory;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Generator;

final readonly class BoundedRead
{
    public function __construct(
        private Unit $limit,
        private Backend $backend,
    ) {}

    /**
     * @param list<Rows> $read
     * @param Generator<Rows> $rest
     *
     * @return Generator<Rows> the batches already read, then what is left of $rest
     */
    public function followedBy(array $read, Generator $rest): Generator
    {
        yield from $read;

        // a started generator is never rewound, and PHP refuses to yield from one that already finished
        if ($rest->valid()) {
            yield from $rest;
        }
    }

    /**
     * Reads $rows while the process stays under the limit; $rows is left at the first batch not read.
     *
     * @param Generator<Rows> $rows
     *
     * @return array{list<Rows>, bool} the batches read, and whether $rows ended within the limit
     */
    public function read(Generator $rows): array
    {
        $budget = new MemoryBudget($this->backend, $this->limit);
        $read = [];

        while ($rows->valid()) {
            /** @var Rows $batch valid() holds, so current() is a batch */
            $batch = $rows->current();
            $read[] = $batch;
            $rows->next();

            if ($budget->exceeded()) {
                return [$read, false];
            }
        }

        return [$read, true];
    }
}

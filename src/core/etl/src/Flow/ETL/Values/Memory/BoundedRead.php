<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Memory;

use Flow\ETL\Values\Rows;
use Generator;

final readonly class BoundedRead
{
    public function __construct(
        private MemoryBudget $budget,
    ) {}

    /**
     * @param list<Rows> $read
     * @param Generator<Rows> $rest
     *
     * @return Generator<Rows> the batches already read, then what is left of $rest
     */
    public static function followedBy(array $read, Generator $rest): Generator
    {
        yield from $read;

        // a started generator is never rewound, and PHP refuses to yield from one that already finished
        if ($rest->valid()) {
            yield from $rest;
        }
    }

    /**
     * Reads $rows while the operation stays under its budget; $rows is left at the first batch not read.
     *
     * @param Generator<Rows> $rows
     *
     * @return array{list<Rows>, bool} the batches read, and whether $rows ended within the limit
     */
    public function read(Generator $rows): array
    {
        $read = [];

        while ($rows->valid()) {
            /** @var Rows $batch valid() holds, so current() is a batch */
            $batch = $rows->current();
            $read[] = $batch;
            $rows->next();

            if ($this->budget->exceeded()) {
                return [$read, false];
            }
        }

        return [$read, true];
    }
}

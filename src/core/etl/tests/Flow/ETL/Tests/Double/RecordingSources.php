<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Row\RawRowValues;
use Generator;

use function array_keys;

final class RecordingSources
{
    /**
     * Indices in the order their first row was pulled - a source is "started" only when its generator body runs.
     *
     * @var list<int>
     */
    public array $started = [];

    /**
     * @var array<int, int>
     */
    public array $rowsRead = [];

    /**
     * @param list<list<RawRowValues>> $sources
     */
    public function __construct(
        private readonly array $sources,
    ) {}

    /**
     * @return Generator<int, RawRowValues>
     */
    public function source(int $index): Generator
    {
        $this->started[] = $index;
        $this->rowsRead[$index] = 0;

        foreach ($this->sources[$index] as $row) {
            $this->rowsRead[$index]++;

            yield $row;
        }
    }

    /**
     * @return Generator<int, Generator<int, RawRowValues>>
     */
    public function sources(): Generator
    {
        foreach (array_keys($this->sources) as $index) {
            yield $this->source($index);
        }
    }
}

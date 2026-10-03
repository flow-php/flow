<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Generator;

use function array_slice;

/**
 * Collects rows sharing a key into one batch, so a downstream operator sees each key exactly once
 * and contiguously. The normalized hash is the key: no tuple, and `1`, `1.0` and `"1"` land together.
 */
final readonly class KeyGrouping
{
    public function __construct(
        private KeyValues $keys,
        private Hasher $hasher,
        private Backend $backend,
    ) {}

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function group(Generator $rows): Generator
    {
        /** @var array<string, list<Rows>> $groups each key's rows, one gathered part per batch, in batch order */
        $groups = [];
        $schema = null;

        foreach ($rows as $input) {
            if (!$input->count()) {
                continue;
            }

            // bound once: every batch out of one bucket shares a schema
            $schema ??= $input->schema();
            $batch = $input->matchTo($schema, $this->backend);

            /** @var array<string, list<int>> $indices */
            $indices = [];

            foreach ($this->hasher->hash($this->keys->of($batch)) as $i => $hash) {
                $indices[$hash][] = $i;
            }

            foreach ($indices as $hash => $rowsOfKey) {
                $groups[$hash][] = $batch->gather($rowsOfKey);
            }
        }

        foreach ($groups as $parts) {
            yield $parts[0]->concat($this->backend, ...array_slice($parts, 1));
        }
    }
}

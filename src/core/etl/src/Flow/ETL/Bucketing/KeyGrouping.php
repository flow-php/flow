<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Rows;
use Generator;

use function array_values;

/**
 * Collects rows sharing a key into one batch, so a downstream operator sees each key exactly once
 * and contiguously. The normalized hash is the key: no tuple, and `1`, `1.0` and `"1"` land together.
 */
final readonly class KeyGrouping
{
    public function __construct(
        private KeyValues $keys,
        private Hasher $hasher,
    ) {}

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<Rows>
     */
    public function group(Generator $rows): Generator
    {
        /** @var array<string, list<\Flow\ETL\Row>> $groups */
        $groups = [];
        $schema = null;

        foreach ($rows as $batch) {
            if (!$batch->count()) {
                continue;
            }

            // bound once, like BucketAggregation: every batch out of one bucket shares a schema
            $schema ??= $batch->schema();
            $hashes = $this->hasher->hash($this->keys->of($batch));

            foreach (array_values($batch->all()) as $index => $row) {
                $groups[$hashes[$index]][] = $row;
            }
        }

        if ($schema === null) {
            return;
        }

        foreach ($groups as $rowsOfKey) {
            yield new Rows($schema, ...$rowsOfKey);
        }
    }
}

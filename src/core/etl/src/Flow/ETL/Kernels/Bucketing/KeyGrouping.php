<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Bucketing;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Generator;

/**
 * Collects rows sharing a key into one batch, so a downstream operator sees each key exactly once
 * and contiguously.
 */
final readonly class KeyGrouping
{
    public function __construct(
        private KeyEncoder $keys,
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

            foreach (self::indices($this->keys->of($batch)) as $key => $rowsOfKey) {
                $groups[$key][] = $batch->gather($rowsOfKey);
            }
        }

        foreach ($groups as $parts) {
            yield Rows::concat($parts[0]->schema(), $this->backend, ...$parts);
        }
    }

    /**
     * Each key's rows, by the key, in row order. One pass per row: with no hash per row it beats gathering by value
     * (one array_keys() pass per distinct key) from 4 distinct keys on, 0.03 s against 0.11 s at 24 on 1 M rows. A key
     * never reads as an integer array key: every one starts with a NUL byte, or is empty.
     *
     * @param list<string> $keys one key per row
     *
     * @return array<string, list<int>>
     */
    public static function indices(array $keys): array
    {
        $indices = [];

        foreach ($keys as $i => $key) {
            $indices[$key][] = $i;
        }

        return $indices;
    }
}

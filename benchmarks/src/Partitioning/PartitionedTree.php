<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Partitioning;

use Flow\Benchmarks\Datasets\FixtureFingerprint;
use Flow\Benchmarks\Datasets\FixtureFormat;
use Flow\Benchmarks\Datasets\Paths;
use RuntimeException;

use function array_key_exists;
use function basename;
use function count;
use function glob;
use function is_dir;
use function strpos;
use function substr;

/**
 * Lives in fixture cache (benchmarks/var is wiped per process); read path must be a glob or from_csv reads 0 rows.
 */
final class PartitionedTree
{
    /** @var array<string, self> */
    private static array $trees = [];

    private function __construct(
        private readonly string $root,
        private readonly string $firstValue,
    ) {}

    public static function clear(): void
    {
        self::$trees = [];
    }

    public static function of(PartitionCardinality $cardinality, int $rows): self
    {
        $key = $cardinality->value . '_' . $rows;

        if (array_key_exists($key, self::$trees)) {
            return self::$trees[$key];
        }

        $root =
            Paths::datasets() . '/partitioned_' . $key . '.' . (new FixtureFingerprint(FixtureFormat::csv))->value();

        if (!is_dir($root)) {
            (new PartitionedOrders($cardinality, $rows))->writeTo($root);
        }

        $directories = glob($root . '/*') ?: [];

        if ($directories === []) {
            throw new RuntimeException('Partitioned write produced no partitions under ' . $root);
        }

        $first = basename($directories[0]);
        $separator = strpos($first, '=');

        if ($separator === false) {
            throw new RuntimeException('Partition directory is not a Hive-style key=value pair: ' . $first);
        }

        return self::$trees[$key] = new self($root, substr($first, $separator + 1));
    }

    public function firstValue(): string
    {
        return $this->firstValue;
    }

    public function glob(): string
    {
        return $this->root . '/**/*.csv';
    }

    public function partitionCount(): int
    {
        return count(glob($this->root . '/*') ?: []);
    }

    public function root(): string
    {
        return $this->root;
    }
}

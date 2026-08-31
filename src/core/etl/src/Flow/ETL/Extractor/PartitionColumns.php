<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;

use function array_key_exists;
use function array_keys;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\str_schema;
use function ksort;

final readonly class PartitionColumns
{
    public function __construct(
        private Filesystem $filesystem,
    ) {}

    /**
     * One read yields one Schema, so a partition column that only some paths under the listing carry
     * still has to be declared for all of them - and it is nullable exactly when some path lacks it.
     * Partition values live in the path, so both the union and its nullability are known without
     * opening a single file.
     *
     * @return array<string, bool> partition name => nullable
     */
    public function names(Path $path, Filter $filter): array
    {
        $counts = [];
        $paths = 0;

        foreach ((new FileListing($this->filesystem))->list($path, $filter) as $status) {
            $paths++;

            foreach ($status->path->partitions() as $partition) {
                $counts[$partition->name] = ($counts[$partition->name] ?? 0) + 1;
            }
        }

        $names = [];

        foreach ($counts as $name => $count) {
            $names[$name] = $count < $paths;
        }

        ksort($names);

        return $names;
    }

    /**
     * A partition column keeps the type its declared definition gives it, but never its body
     * position: it is removed from wherever the file put it and re-appended in the partition block,
     * so a declared read and an undeclared one emit the same column order.
     *
     * @param array<string, bool> $names
     */
    public function declare(Schema $schema, array $names, PartitionTypes $types = new PartitionTypes()): Schema
    {
        $types->assertEveryNameIsAPartition($names);

        if ($names === []) {
            return $schema;
        }

        $definitions = [];

        foreach ($names as $name => $nullable) {
            // 1) a declared schema wins, 2) then a declared partition type, 3) then string
            $definitions[] =
                $schema->findDefinition($name)
                ?? (
                    $types->has($name)
                        ? definition_from_type($name, $types->get($name), nullable: $nullable)
                        : str_schema($name, nullable: $nullable)
                );
        }

        return $schema->gracefulRemove(...array_keys($names))->add(...$definitions);
    }

    /**
     * Inference sees one stream at a time, so it types a partition column from that stream alone -
     * `string` where the path carries it, `?null` where it does not, and the two batches then refuse
     * to merge. The path knows better than the values do, so here the partition definition wins.
     *
     * @param array<string, bool> $names
     */
    public function apply(Rows $rows, array $names, PartitionTypes $types = new PartitionTypes()): Rows
    {
        $types->assertEveryNameIsAPartition($names);

        if ($names === []) {
            return $rows;
        }

        $definitions = [];

        foreach ($names as $name => $nullable) {
            $definitions[] = $types->has($name)
                ? definition_from_type($name, $types->get($name), nullable: $nullable)
                : str_schema($name, nullable: $nullable);
        }

        return new Rows($rows->schema()->gracefulRemove(...array_keys($names))->add(...$definitions), ...$rows->all());
    }

    /**
     * @param array<string, bool> $names
     * @param array<string, null|string> $values partition name => value, for the path this row came from
     * @param array<string, mixed> $row
     *
     * @return array<string, mixed>
     */
    public function fill(array $row, array $names, array $values): array
    {
        foreach ($names as $name => $_) {
            $row[$name] = array_key_exists($name, $values) ? $values[$name] : null;
        }

        return $row;
    }
}

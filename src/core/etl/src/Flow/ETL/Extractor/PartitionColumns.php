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
     * @return array<string, bool> partition name => nullable
     */
    public function names(Path $path, Filter $filter): array
    {
        $counts = [];
        $nullable = [];
        $paths = 0;

        foreach ((new FileListing($this->filesystem))->list($path, $filter) as $status) {
            $paths++;

            foreach ($status->path->partitions() as $partition) {
                $counts[$partition->name] = ($counts[$partition->name] ?? 0) + 1;

                if ($partition->value === null) {
                    $nullable[$partition->name] = true;
                }
            }
        }

        $names = [];

        foreach ($counts as $name => $count) {
            $names[$name] = $count < $paths || array_key_exists($name, $nullable);
        }

        ksort($names);

        return $names;
    }

    /**
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
     * @param array<array-key, bool> $names
     * @param array<array-key, mixed> $values partition name => value, for the path this row came from
     * @param array<array-key, mixed> $row
     *
     * @return array<array-key, mixed>
     */
    public function fill(array $row, array $names, array $values): array
    {
        foreach ($names as $name => $_) {
            unset($row[$name]);
            $row[$name] = array_key_exists($name, $values) ? $values[$name] : null;
        }

        return $row;
    }
}

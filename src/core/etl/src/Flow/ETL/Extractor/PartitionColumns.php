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
use function Flow\ETL\DSL\str_schema;

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

        return $names;
    }

    /**
     * @param array<string, bool> $names
     */
    public function declare(Schema $schema, array $names): Schema
    {
        foreach ($names as $name => $nullable) {
            if ($schema->findDefinition($name) === null) {
                $schema = $schema->add(str_schema($name, nullable: $nullable));
            }
        }

        return $schema;
    }

    /**
     * Inference sees one stream at a time, so it types a partition column from that stream alone -
     * `string` where the path carries it, `?null` where it does not, and the two batches then refuse
     * to merge. The path knows better than the values do, so here the partition definition wins.
     *
     * @param array<string, bool> $names
     */
    public function apply(Rows $rows, array $names): Rows
    {
        $schema = $rows->schema();

        foreach ($names as $name => $nullable) {
            $definition = str_schema($name, nullable: $nullable);

            $schema = $schema->findDefinition($name) === null
                ? $schema->add($definition)
                : $schema->replace($name, $definition);
        }

        return new Rows($schema, ...$rows->all());
    }

    /**
     * @param array<string, bool> $names
     * @param array<string, string> $values partition name => value, for the path this row came from
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

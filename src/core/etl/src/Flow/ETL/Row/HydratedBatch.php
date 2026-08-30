<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;

use function array_key_exists;

/**
 * Builds a batch against a declared Schema. Metadata belongs to the column, so it is folded into the
 * schema once - a per-row divergent Metadata is no longer representable.
 */
final readonly class HydratedBatch
{
    /**
     * @param list<RawRowValues> $batch
     * @param callable(mixed, Definition<mixed>): mixed $prepare
     */
    public function of(array $batch, Schema $schema, callable $prepare, bool $fillMissing): Rows
    {
        foreach ($batch as $rowValues) {
            foreach ($rowValues->metadata as $name => $metadata) {
                // PHP casts a numeric array key to int, column names are always strings
                $name = (string) $name;

                if ($schema->findDefinition($name) !== null) {
                    $schema = $schema->setMetadata($name, $metadata);
                }
            }
        }

        $definitions = $schema->definitions();
        $rows = [];

        foreach ($batch as $rowValues) {
            $values = [];

            foreach ($definitions as $definition) {
                $name = $definition->entry()->name();

                if (!array_key_exists($name, $rowValues->values)) {
                    if ($fillMissing) {
                        $values[$name] = null;
                    }

                    continue;
                }

                $values[$name] = $prepare($rowValues->values[$name], $definition);
            }

            $rows[] = new Row($values);
        }

        return new Rows($schema, ...$rows);
    }
}

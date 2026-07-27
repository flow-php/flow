<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;

use function array_key_exists;

final class PhpRowHydrator implements Hydrator
{
    private readonly EntryFactory $entryFactory;

    public function __construct()
    {
        $this->entryFactory = new EntryFactory();
    }

    public function cast(array $batch, ?Schema $schema = null): Rows
    {
        if ($schema === null) {
            return $this->infer($batch);
        }

        return $this->instantiate(
            $batch,
            $schema,
            static fn(mixed $value, Definition $definition): mixed => $value === null
                ? null
                : $definition->type()->cast($value),
            fillMissing: true,
        );
    }

    public function dehydrate(Rows $rows): array
    {
        $batch = [];

        foreach ($rows as $row) {
            $values = [];
            $types = [];
            $metadata = [];

            foreach ($row->entries()->all() as $entry) {
                $definition = $entry->definition();

                $values[$entry->name()] = $entry->value();
                $types[$entry->name()] = $definition->type();

                if (!$definition->metadata()->isEmpty()) {
                    $metadata[$entry->name()] = $definition->metadata();
                }
            }

            $batch[] = new TypedRowValues($values, $types, $metadata);
        }

        return $batch;
    }

    public function hydrate(array $batch, ?Schema $schema = null): Rows
    {
        if ($schema === null) {
            throw new InvalidArgumentException(
                'PhpRowHydrator::hydrate() requires a schema, use cast() to infer from values',
            );
        }

        return $this->instantiate(
            $batch,
            $schema,
            static fn(mixed $value, Definition $definition): mixed => $value,
            fillMissing: false,
        );
    }

    /**
     * @param list<RawRowValues> $batch
     * @param callable(mixed, Definition<mixed>): mixed $prepare
     */
    private function instantiate(array $batch, Schema $schema, callable $prepare, bool $fillMissing): Rows
    {
        $definitions = $schema->definitions();
        $rows = [];

        foreach ($batch as $rowValues) {
            $entries = [];

            foreach ($definitions as $definition) {
                $name = $definition->entry()->name();

                if (!array_key_exists($name, $rowValues->values)) {
                    if ($fillMissing) {
                        $entries[$name] = $this->entryFactory->fromDefinition($definition, null);
                    }

                    continue;
                }

                if (array_key_exists($name, $rowValues->metadata)) {
                    $definition = $definition->setMetadata($rowValues->metadata[$name]);
                }

                // @mago-ignore analysis:mixed-assignment
                $value = $rowValues->values[$name];

                $entries[$name] = $this->entryFactory->fromDefinition($definition, $prepare($value, $definition));
            }

            $rows[] = new Row(Entries::recreate($entries));
        }

        return new Rows(...$rows);
    }

    /**
     * @param list<RawRowValues> $batch
     */
    private function infer(array $batch): Rows
    {
        $rows = [];

        foreach ($batch as $rowValues) {
            $entries = [];

            /** @var mixed $value */
            foreach ($rowValues->values as $name => $value) {
                $entries[$name] = $this->entryFactory->create(
                    $name,
                    $value,
                    null,
                    array_key_exists($name, $rowValues->metadata) ? $rowValues->metadata[$name] : null,
                );
            }

            $rows[] = new Row(Entries::recreate($entries));
        }

        return new Rows(...$rows);
    }
}

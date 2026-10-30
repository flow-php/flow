<?php

declare(strict_types=1);

namespace Flow\ETL\Values;

use Countable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\ETL\Values\Schema\Metadata;
use Flow\ETL\Values\Schema\SameSchemas;
use Flow\ETL\Values\Schema\SortingStrategy;
use Flow\ETL\Values\Schema\SortingStrategy\AlphabeticalStrategy;

use function array_key_exists;
use function array_keys;
use function array_map;
use function array_search;
use function array_splice;
use function array_values;
use function count;
use function implode;
use function in_array;
use function is_array;
use function sprintf;
use function usort;

final readonly class Schema implements Countable
{
    /**
     * @var array<string, Definition<mixed>>
     */
    private array $definitions;

    /**
     * @param Definition<mixed> ...$definitions
     */
    public function __construct(Definition ...$definitions)
    {
        $this->setDefinitions(...$definitions);
    }

    /**
     * @param array<array-key, mixed> $definitions
     */
    public static function fromArray(array $definitions): self
    {
        $schema = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($definitions as $definition) {
            if (!is_array($definition)) {
                throw new InvalidArgumentException('Schema definition must be an array');
            }

            $schema[] = Definitions::fromArray($definition);
        }

        return new self(...$schema);
    }

    /**
     * @param Definition<mixed> ...$definitions
     *
     * @return Schema
     */
    public function add(Definition ...$definitions): self
    {
        return new self(...array_values($this->definitions), ...$definitions);
    }

    /**
     * Adds $definition, or replaces the definition of the same name in place.
     *
     * @param Definition<mixed> $definition
     */
    public function put(Definition $definition): self
    {
        $name = $definition->name();

        return $this->findDefinition($name) === null ? $this->add($definition) : $this->replace($name, $definition);
    }

    /**
     * Inserts definitions immediately after an existing column.
     *
     * @param Definition<mixed> ...$definitions
     *
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function addAfter(string $reference, Definition ...$definitions): self
    {
        return $this->insertAt($this->indexOf($reference) + 1, ...$definitions);
    }

    /**
     * Inserts definitions immediately before an existing column.
     *
     * @param Definition<mixed> ...$definitions
     *
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function addBefore(string $reference, Definition ...$definitions): self
    {
        return $this->insertAt($this->indexOf($reference), ...$definitions);
    }

    /**
     * Adds metadata to a given definition.
     *
     * @param array<array-key, mixed>|bool|float|int|string $value
     *
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function addMetadata(string $definition, string $name, int|string|bool|float|array $value): self
    {
        return $this->replace($definition, $this->get($definition)->addMetadata($name, $value));
    }

    /**
     * Columns present in both schemas are emitted in $authority's order; columns only in $this are
     * appended unchanged. Nothing is added, dropped or widened, and no type is rewritten - a column
     * whose type orders its own elements differently is a different type, not a reorderable one.
     */
    public function matchOrderTo(self $authority): self
    {
        $definitions = [];

        foreach ($authority->definitions() as $authorityDefinition) {
            $definition = $this->findDefinition($authorityDefinition->name());

            if ($definition === null) {
                continue;
            }

            $definitions[] = $definition;
        }

        foreach ($this->definitions as $definition) {
            if ($authority->findDefinition($definition->name()) === null) {
                $definitions[] = $definition;
            }
        }

        return new self(...$definitions);
    }

    public function count(): int
    {
        return count($this->definitions);
    }

    /**
     * @return array<string, Definition<mixed>>
     */
    public function definitions(): array
    {
        return $this->definitions;
    }

    /**
     * @return null|Definition<mixed>
     */
    public function findDefinition(string $name): ?Definition
    {
        if (!array_key_exists($name, $this->definitions)) {
            return null;
        }

        return $this->definitions[$name];
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Definition<mixed>
     */
    public function get(string $name): Definition
    {
        return (
            $this->findDefinition($name)
            ?: throw SchemaDefinitionNotFoundException::withAvailable($name, ...$this->names())
        );
    }

    /**
     * Gracefully remove columns from schema without throwing an exception if column does not exist.
     */
    public function gracefulRemove(string ...$names): self
    {
        $definitions = [];

        foreach ($this->definitions as $definition) {
            if (!in_array($definition->name(), $names, true)) {
                $definitions[] = $definition;
            }
        }

        return new self(...$definitions);
    }

    /**
     * Inserts definitions at an explicit position. Index 0 prepends, an index equal to the
     * number of definitions appends.
     *
     * @param Definition<mixed> ...$definitions
     *
     * @throws InvalidArgumentException
     *
     * @return Schema
     */
    public function insertAt(int $index, Definition ...$definitions): self
    {
        $definitionsList = array_values($this->definitions);

        if ($index < 0 || $index > count($definitionsList)) {
            throw new InvalidArgumentException(sprintf(
                'Cannot insert definitions at index %d, schema has %d definitions',
                $index,
                count($definitionsList),
            ));
        }

        array_splice($definitionsList, $index, 0, $definitions);

        return new self(...$definitionsList);
    }

    /**
     * Column order is part of a Schema's identity - two schemas holding the same columns in a
     * different order describe two different row shapes, and a row is stored in Schema order.
     */
    public function isSame(self $schema): bool
    {
        if ($this === $schema || SameSchemas::proved($this, $schema)) {
            return true;
        }

        if (array_keys($this->definitions) !== array_keys($schema->definitions)) {
            return false;
        }

        foreach ($this->definitions as $column => $definition) {
            if (!$definition->isSame($schema->definitions[$column])) {
                return false;
            }
        }

        SameSchemas::remember($this, $schema);

        return true;
    }

    /**
     * @return Schema
     */
    public function keep(string ...$names): self
    {
        $definitions = [];

        foreach ($names as $name) {
            if (!$this->findDefinition($name)) {
                throw SchemaDefinitionNotFoundException::withAvailable($name, ...$this->names());
            }
        }

        foreach ($this->definitions as $definition) {
            if (in_array($definition->name(), $names, true)) {
                $definitions[] = $definition;
            }
        }

        return new self(...$definitions);
    }

    /**
     * Makes all schema definitions nullable.
     */
    public function makeNullable(): self
    {
        $definitions = [];

        foreach ($this->definitions as $definition) {
            if (!$definition->isNullable()) {
                $definitions[] = $definition->makeNullable();
            } else {
                $definitions[] = $definition;
            }
        }

        return new self(...$definitions);
    }

    public function merge(self $schema): self
    {
        if (!$this->count()) {
            return $schema;
        }

        if (!$schema->count()) {
            return $this;
        }

        if ($this->isSame($schema)) {
            return $this;
        }

        $newDefinitions = $this->definitions;

        foreach ($schema->definitions as $column => $definition) {
            if (!array_key_exists($definition->name(), $newDefinitions)) {
                $newDefinitions[$column] = $definition->makeNullable();
            } else {
                $newDefinitions[$column] = $newDefinitions[$column]->merge($definition);
            }
        }

        foreach ($newDefinitions as $column => $definition) {
            if (!array_key_exists($definition->name(), $schema->definitions)) {
                $newDefinitions[$column] = $definition->makeNullable();
            }
        }

        return new self(...array_values($newDefinitions));
    }

    /**
     * Moves an existing column to immediately after another column, preserving its definition.
     *
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function moveAfter(string $name, string $reference): self
    {
        return $this->moveRelative($name, $reference, 1);
    }

    /**
     * Moves an existing column to immediately before another column, preserving its definition.
     *
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function moveBefore(string $name, string $reference): self
    {
        return $this->moveRelative($name, $reference, 0);
    }

    /**
     * Moves an existing column to an explicit position, preserving its definition.
     * The index is the final position of the column in the resulting schema.
     *
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function moveTo(string $name, int $index): self
    {
        $from = $this->indexOf($name);
        $definitionsList = array_values($this->definitions);

        if ($index < 0 || $index >= count($definitionsList)) {
            throw new InvalidArgumentException(sprintf(
                'Cannot move column "%s" to index %d, schema has %d definitions',
                $name,
                $index,
                count($definitionsList),
            ));
        }

        array_splice($definitionsList, $index, 0, array_splice($definitionsList, $from, 1));

        return new self(...$definitionsList);
    }

    /**
     * @return array<array-key, array<mixed>>
     */
    public function normalize(): array
    {
        $definitions = [];

        foreach ($this->definitions as $definition) {
            $definitions[] = $definition->normalize();
        }

        return $definitions;
    }

    /**
     * Inserts definitions at the beginning of the schema.
     *
     * @param Definition<mixed> ...$definitions
     *
     * @return Schema
     */
    public function prepend(Definition ...$definitions): self
    {
        return new self(...$definitions, ...array_values($this->definitions));
    }

    /**
     * @return list<string>
     */
    public function names(): array
    {
        $names = [];

        foreach ($this->definitions as $definition) {
            $names[] = $definition->name();
        }

        return $names;
    }

    /**
     * @return Schema
     */
    public function remove(string ...$names): self
    {
        $definitions = [];

        foreach ($names as $name) {
            if (!$this->findDefinition($name)) {
                throw SchemaDefinitionNotFoundException::withAvailable($name, ...$this->names());
            }
        }

        foreach ($this->definitions as $definition) {
            if (!in_array($definition->name(), $names, true)) {
                $definitions[] = $definition;
            }
        }

        return new self(...$definitions);
    }

    /**
     * @return Schema
     */
    public function rename(string $column, string $newName): self
    {
        $definitions = [];

        if (!$this->findDefinition($column)) {
            throw SchemaDefinitionNotFoundException::withAvailable($column, ...$this->names());
        }

        foreach ($this->definitions as $nextDefinition) {
            if ($nextDefinition->name() === $column) {
                $definitions[] = $nextDefinition->rename($newName);
            } else {
                $definitions[] = $nextDefinition;
            }
        }

        return new self(...$definitions);
    }

    /**
     * Reorders columns by name. Any columns not listed keep their relative order and are appended.
     *
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function reorder(string ...$names): self
    {
        $definitions = [];
        $reordered = [];

        foreach ($names as $name) {
            $definition = $this->findDefinition($name)
            ?: throw SchemaDefinitionNotFoundException::withAvailable($name, ...$this->names());
            $key = $definition->name();

            if (array_key_exists($key, $reordered)) {
                throw new InvalidArgumentException(sprintf('Cannot reorder column "%s" more than once', $name));
            }

            $reordered[$key] = true;
            $definitions[] = $definition;
        }

        foreach ($this->definitions as $key => $definition) {
            if (!array_key_exists($key, $reordered)) {
                $definitions[] = $definition;
            }
        }

        return new self(...$definitions);
    }

    /**
     * @param Definition<mixed> $definition
     *
     * @return Schema
     */
    public function replace(string $column, Definition $definition): self
    {
        $definitions = [];

        if (!$this->findDefinition($column)) {
            throw SchemaDefinitionNotFoundException::withAvailable($column, ...$this->names());
        }

        foreach ($this->definitions as $nextDefinition) {
            if ($nextDefinition->name() === $column) {
                $definitions[] = $definition;
            } else {
                $definitions[] = $nextDefinition;
            }
        }

        return new self(...$definitions);
    }

    /**
     * Overwrites metadata for a given definition.
     *
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function setMetadata(string $definition, Metadata $metadata): self
    {
        return $this->replace($definition, $this->get($definition)->setMetadata($metadata));
    }

    /**
     * @return Schema
     */
    public function sort(SortingStrategy $strategy = new AlphabeticalStrategy()): self
    {
        $definitions = array_values($this->definitions);

        usort($definitions, static fn(Definition $left, Definition $right): int => $strategy->compare($left, $right));

        return new self(...$definitions);
    }

    private function indexOf(string $reference): int
    {
        $index = array_search($reference, array_keys($this->definitions), true);

        if ($index === false) {
            throw SchemaDefinitionNotFoundException::withAvailable($reference, ...$this->names());
        }

        return $index;
    }

    private function moveRelative(string $name, string $reference, int $offset): self
    {
        $from = $this->indexOf($name);
        $referenceName = $reference;

        if (!$this->findDefinition($reference)) {
            throw SchemaDefinitionNotFoundException::withAvailable($reference, ...$this->names());
        }

        if ($name === $referenceName) {
            throw new InvalidArgumentException(sprintf('Cannot move column "%s" relative to itself', $name));
        }

        $definitionsList = array_values($this->definitions);

        $moved = array_splice($definitionsList, $from, 1);

        $referenceIndex = 0;

        foreach ($definitionsList as $position => $definition) {
            if ($definition->name() === $referenceName) {
                $referenceIndex = $position;

                break;
            }
        }

        array_splice($definitionsList, $referenceIndex + $offset, 0, $moved);

        return new self(...$definitionsList);
    }

    /**
     * @param Definition<mixed> ...$definitions
     */
    private function setDefinitions(Definition ...$definitions): void
    {
        $uniqueDefinitions = [];
        $duplicatedDefinitions = [];

        foreach ($definitions as $definition) {
            if (array_key_exists($definition->name(), $uniqueDefinitions)) {
                $duplicatedDefinitions[] = $definition->name();
            }
            $uniqueDefinitions[$definition->name()] = $definition;
        }

        if (count($uniqueDefinitions) !== count($definitions)) {
            throw new SchemaDefinitionNotUniqueException(sprintf(
                'Column definitions must be unique, duplicated columns: [%s], all: [%s]',
                implode(', ', $duplicatedDefinitions),
                implode(', ', array_map(static fn(Definition $d) => $d->name(), $definitions)),
            ));
        }

        $this->definitions = $uniqueDefinitions;
    }
}

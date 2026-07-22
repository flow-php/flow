<?php

declare(strict_types=1);

namespace Flow\ETL;

use Countable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Schema\SortingStrategy;
use Flow\ETL\Schema\SortingStrategy\AlphabeticalStrategy;

use function array_key_exists;
use function array_keys;
use function array_map;
use function array_merge;
use function array_search;
use function array_splice;
use function array_values;
use function count;
use function Flow\ETL\DSL\definition_from_array;
use function Flow\ETL\DSL\schema;
use function implode;
use function is_array;
use function sprintf;
use function usort;

final class Schema implements Countable
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

            $schema[] = definition_from_array($definition);
        }

        return new self(...$schema);
    }

    /**
     * Detecting schema from the pipeline has several disadvantages.
     * First of all, it's expensive, it needs to iterate through the pipeline until it detects
     * types of all columns.
     * In some cases, when a given column is null in the first 1k rows it will anyway return incorrect
     * schema since row 1001 might have an actual value.
     * When dealing with schemaless file formats like CSV or JSON even when first 1k rows will
     * carry value of one type, there is zero guarantee that following rows will do the same.
     *
     * Whenever it's possible, it's recommended to define schema upfront and pass it to the extractor.
     * This way, whatever process would need to use this method, will do just one iteration.
     */
    public static function fromPipeline(Pipeline $pipeline, FlowContext $context, int $maxRows = 1000): self
    {
        if ($maxRows <= 0) {
            throw new InvalidArgumentException('Total numbers of rows to scan must be a positive number');
        }

        $extractor = $pipeline->process($context);
        $schema = schema();
        $totalRows = 0;

        foreach ($extractor as $rows) {
            foreach ($rows as $row) {
                $schema = $schema->merge($row->schema());
                $totalRows++;

                if ($totalRows >= $maxRows) {
                    return $schema;
                }

                $allDetected = true;

                foreach ($schema->definitions() as $definition) {
                    if ($definition instanceof NullDefinition) {
                        $allDetected = false;

                        break;
                    }
                }

                if ($allDetected) {
                    return $schema;
                }
            }
        }

        return $schema;
    }

    /**
     * @param Definition<mixed> ...$definitions
     *
     * @return Schema
     */
    public function add(Definition ...$definitions): self
    {
        $this->setDefinitions(...array_merge(array_values($this->definitions), $definitions));

        return $this;
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
    public function addAfter(string|Reference $reference, Definition ...$definitions): self
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
    public function addBefore(string|Reference $reference, Definition ...$definitions): self
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
        $this->get($definition)->addMetadata($name, $value);

        return $this;
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
    public function findDefinition(string|Reference $ref): ?Definition
    {
        if ($ref instanceof Reference) {
            if (!array_key_exists($ref->name(), $this->definitions)) {
                return null;
            }

            return $this->definitions[$ref->name()];
        }

        if (!array_key_exists($ref, $this->definitions)) {
            return null;
        }

        return $this->definitions[$ref];
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Definition<mixed>
     */
    public function get(string|Reference $ref): Definition
    {
        return $this->findDefinition($ref) ?: throw new SchemaDefinitionNotFoundException((string) $ref);
    }

    /**
     * Gracefully remove entries from schema without throwing an exception if entry does not exist.
     */
    public function gracefulRemove(string|Reference ...$entries): self
    {
        $refs = References::init(...$entries);

        $definitions = [];

        foreach ($this->definitions as $definition) {
            if (!$refs->has($definition->entry())) {
                $definitions[] = $definition;
            }
        }

        $this->setDefinitions(...$definitions);

        return $this;
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

        $this->setDefinitions(...$definitionsList);

        return $this;
    }

    public function isSame(self $schema): bool
    {
        if (count($this->definitions) !== count($schema->definitions)) {
            return false;
        }

        foreach ($this->definitions as $entry => $definition) {
            if (!array_key_exists($entry, $schema->definitions)) {
                return false;
            }

            if (!$definition->isSame($schema->definitions[$entry])) {
                return false;
            }
        }

        return true;
    }

    /**
     * @return Schema
     */
    public function keep(string|Reference ...$entries): self
    {
        $refs = References::init(...$entries);

        $definitions = [];

        foreach ($entries as $entry) {
            if (!$this->findDefinition($entry)) {
                throw new SchemaDefinitionNotFoundException((string) $entry);
            }
        }

        foreach ($this->definitions as $definition) {
            if ($refs->has($definition->entry())) {
                $definitions[] = $definition;
            }
        }

        $this->setDefinitions(...$definitions);

        return $this;
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

        $this->setDefinitions(...$definitions);

        return $this;
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

        foreach ($schema->definitions as $entry => $definition) {
            if (!array_key_exists($definition->entry()->name(), $newDefinitions)) {
                $newDefinitions[$entry] = $definition->makeNullable();
            } else {
                $newDefinitions[$entry] = $newDefinitions[$entry]->merge($definition);
            }
        }

        foreach ($newDefinitions as $entry => $definition) {
            if (!array_key_exists($definition->entry()->name(), $schema->definitions)) {
                $newDefinitions[$entry] = $definition->makeNullable();
            }
        }

        $this->setDefinitions(...array_values($newDefinitions));

        return $this;
    }

    /**
     * Moves an existing column to immediately after another column, preserving its definition.
     *
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function moveAfter(string|Reference $name, string|Reference $reference): self
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
    public function moveBefore(string|Reference $name, string|Reference $reference): self
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
    public function moveTo(string|Reference $name, int $index): self
    {
        $from = $this->indexOf($name);
        $definitionsList = array_values($this->definitions);

        if ($index < 0 || $index >= count($definitionsList)) {
            throw new InvalidArgumentException(sprintf(
                'Cannot move entry "%s" to index %d, schema has %d definitions',
                (string) $name,
                $index,
                count($definitionsList),
            ));
        }

        $moved = array_splice($definitionsList, $from, 1);
        array_splice($definitionsList, $index, 0, $moved);

        $this->setDefinitions(...$definitionsList);

        return $this;
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
        $this->setDefinitions(...$definitions, ...array_values($this->definitions));

        return $this;
    }

    public function references(): References
    {
        $refs = [];

        foreach ($this->definitions as $definition) {
            $refs[] = $definition->entry();
        }

        return References::init(...$refs);
    }

    /**
     * @return Schema
     */
    public function remove(string|Reference ...$entries): self
    {
        $refs = References::init(...$entries);

        $definitions = [];

        foreach ($entries as $entry) {
            if (!$this->findDefinition($entry)) {
                throw new SchemaDefinitionNotFoundException((string) $entry);
            }
        }

        foreach ($this->definitions as $definition) {
            if (!$refs->has($definition->entry())) {
                $definitions[] = $definition;
            }
        }

        $this->setDefinitions(...$definitions);

        return $this;
    }

    /**
     * @return Schema
     */
    public function rename(string|Reference $entry, string $newName): self
    {
        $definitions = [];

        if (!$this->findDefinition($entry)) {
            throw new SchemaDefinitionNotFoundException((string) $entry);
        }

        foreach ($this->definitions as $nextDefinition) {
            if ($nextDefinition->entry()->is(EntryReference::init($entry))) {
                $definitions[] = $nextDefinition->rename($newName);
            } else {
                $definitions[] = $nextDefinition;
            }
        }

        $this->setDefinitions(...$definitions);

        return $this;
    }

    /**
     * Reorders columns by name. Any columns not listed keep their relative order and are appended.
     *
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function reorder(string|Reference ...$names): self
    {
        $definitions = [];
        $reordered = [];

        foreach ($names as $name) {
            $definition = $this->findDefinition($name) ?: throw new SchemaDefinitionNotFoundException((string) $name);
            $key = $definition->entry()->name();

            if (array_key_exists($key, $reordered)) {
                throw new InvalidArgumentException(sprintf('Cannot reorder entry "%s" more than once', (string) $name));
            }

            $reordered[$key] = true;
            $definitions[] = $definition;
        }

        foreach ($this->definitions as $key => $definition) {
            if (!array_key_exists($key, $reordered)) {
                $definitions[] = $definition;
            }
        }

        $this->setDefinitions(...$definitions);

        return $this;
    }

    /**
     * @param Definition<mixed> $definition
     *
     * @return Schema
     */
    public function replace(string|Reference $entry, Definition $definition): self
    {
        $definitions = [];

        if (!$this->findDefinition($entry)) {
            throw new SchemaDefinitionNotFoundException((string) $entry);
        }

        foreach ($this->definitions as $nextDefinition) {
            if ($nextDefinition->entry()->is(EntryReference::init($entry))) {
                $definitions[] = $definition;
            } else {
                $definitions[] = $nextDefinition;
            }
        }

        $this->setDefinitions(...$definitions);

        return $this;
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
        $this->get($definition)->setMetadata($metadata);

        return $this;
    }

    /**
     * @return Schema
     */
    public function sort(SortingStrategy $strategy = new AlphabeticalStrategy()): self
    {
        $definitions = array_values($this->definitions);

        usort($definitions, static fn(Definition $left, Definition $right): int => $strategy->compare($left, $right));

        $this->setDefinitions(...$definitions);

        return $this;
    }

    private function indexOf(string|Reference $reference): int
    {
        $index = array_search(EntryReference::init($reference)->name(), array_keys($this->definitions), true);

        if ($index === false) {
            throw new SchemaDefinitionNotFoundException((string) $reference);
        }

        return $index;
    }

    private function moveRelative(string|Reference $name, string|Reference $reference, int $offset): self
    {
        $from = $this->indexOf($name);
        $referenceName = EntryReference::init($reference)->name();

        if (!$this->findDefinition($reference)) {
            throw new SchemaDefinitionNotFoundException((string) $reference);
        }

        if (EntryReference::init($name)->name() === $referenceName) {
            throw new InvalidArgumentException(sprintf('Cannot move entry "%s" relative to itself', (string) $name));
        }

        $definitionsList = array_values($this->definitions);

        $moved = array_splice($definitionsList, $from, 1);

        $referenceIndex = 0;

        foreach ($definitionsList as $position => $definition) {
            if ($definition->entry()->name() === $referenceName) {
                $referenceIndex = $position;

                break;
            }
        }

        array_splice($definitionsList, $referenceIndex + $offset, 0, $moved);

        $this->setDefinitions(...$definitionsList);

        return $this;
    }

    /**
     * @param Definition<mixed> ...$definitions
     */
    private function setDefinitions(Definition ...$definitions): void
    {
        $uniqueDefinitions = [];
        $duplicatedDefinitions = [];

        foreach ($definitions as $definition) {
            if (array_key_exists($definition->entry()->name(), $uniqueDefinitions)) {
                $duplicatedDefinitions[] = $definition->entry()->name();
            }
            $uniqueDefinitions[$definition->entry()->name()] = $definition;
        }

        if (count($uniqueDefinitions) !== count($definitions)) {
            throw new SchemaDefinitionNotUniqueException(sprintf(
                'Entry definitions must be unique, duplicated entries: [%s], all: [%s]',
                implode(', ', $duplicatedDefinitions),
                implode(', ', array_map(static fn(Definition $d) => $d->entry()->name(), $definitions)),
            ));
        }

        $this->definitions = $uniqueDefinitions;
    }
}

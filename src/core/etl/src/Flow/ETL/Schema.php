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
use Flow\ETL\Schema\Metadata;

use function array_key_exists;
use function array_map;
use function array_merge;
use function array_splice;
use function array_values;
use function count;
use function Flow\ETL\DSL\definition_from_array;
use function Flow\ETL\DSL\schema;
use function implode;
use function is_array;
use function is_int;
use function sprintf;
use function str_starts_with;
use function substr;

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
                    if ($definition->metadata()->has(Metadata::FROM_NULL)) {
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
     * Inserts new definitions right after an existing one.
     *
     * @param Definition<mixed> ...$definitions
     *
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function addAfter(string|Reference $reference, Definition ...$definitions): self
    {
        $this->get($reference);

        $target = EntryReference::init($reference);
        $result = [];

        foreach (array_values($this->definitions) as $definition) {
            $result[] = $definition;

            if ($definition->entry()->is($target)) {
                foreach ($definitions as $new) {
                    $result[] = $new;
                }
            }
        }

        $this->setDefinitions(...$result);

        return $this;
    }

    /**
     * Inserts new definitions right before an existing one.
     *
     * @param Definition<mixed> ...$definitions
     *
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function addBefore(string|Reference $reference, Definition ...$definitions): self
    {
        $this->get($reference);

        $target = EntryReference::init($reference);
        $result = [];

        foreach (array_values($this->definitions) as $definition) {
            if ($definition->entry()->is($target)) {
                foreach ($definitions as $new) {
                    $result[] = $new;
                }
            }

            $result[] = $definition;
        }

        $this->setDefinitions(...$result);

        return $this;
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
     * Inserts new definitions at an explicit position (0 = beginning, count() = end).
     *
     * @param Definition<mixed> ...$definitions
     *
     * @throws InvalidArgumentException
     *
     * @return Schema
     */
    public function insertAt(int $index, Definition ...$definitions): self
    {
        $current = array_values($this->definitions);

        if ($index < 0 || $index > count($current)) {
            throw InvalidArgumentException::because(
                'Cannot insert definitions at index %d, schema has %d definition(s)',
                $index,
                count($current),
            );
        }

        array_splice($current, $index, 0, $definitions);

        $this->setDefinitions(...$current);

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
     * Relocates an existing definition (preserving its metadata) to a new position.
     * The position is either a numeric index (the final position after removal; use count() - 1 for
     * the end) or a string anchor prefixed with "before:" / "after:" referencing another column.
     *
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function move(string|Reference $name, int|string $position): self
    {
        $definition = $this->get($name);
        $movedName = $definition->entry()->name();

        $remaining = [];

        foreach (array_values($this->definitions) as $next) {
            if ($next->entry()->name() !== $movedName) {
                $remaining[] = $next;
            }
        }

        if (is_int($position)) {
            if ($position < 0 || $position > count($remaining)) {
                throw InvalidArgumentException::because(
                    'Cannot move "%s" to index %d, schema has %d definition(s)',
                    $movedName,
                    $position,
                    count($this->definitions),
                );
            }

            array_splice($remaining, $position, 0, [$definition]);

            $this->setDefinitions(...$remaining);

            return $this;
        }

        if (str_starts_with($position, 'before:')) {
            $anchor = EntryReference::init(substr($position, 7));
            $before = true;
        } elseif (str_starts_with($position, 'after:')) {
            $anchor = EntryReference::init(substr($position, 6));
            $before = false;
        } else {
            throw InvalidArgumentException::because(
                'Move position must be an integer index or a string prefixed with "before:" or "after:", given: "%s"',
                $position,
            );
        }

        if ($anchor->name() === $movedName) {
            throw InvalidArgumentException::because('Cannot move "%s" relative to itself', $movedName);
        }

        $result = [];
        $found = false;

        foreach ($remaining as $next) {
            if ($before && $next->entry()->is($anchor)) {
                $result[] = $definition;
                $found = true;
            }

            $result[] = $next;

            if (!$before && $next->entry()->is($anchor)) {
                $result[] = $definition;
                $found = true;
            }
        }

        if (!$found) {
            throw new SchemaDefinitionNotFoundException($anchor->name());
        }

        $this->setDefinitions(...$result);

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
     * Inserts new definitions at the beginning of the schema.
     *
     * @param Definition<mixed> ...$definitions
     *
     * @return Schema
     */
    public function prepend(Definition ...$definitions): self
    {
        $this->setDefinitions(...array_merge($definitions, array_values($this->definitions)));

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
     * Reorders definitions by name. Any unlisted definitions keep their relative order and are
     * appended after the listed ones.
     *
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Schema
     */
    public function reorder(string|Reference ...$names): self
    {
        $ordered = [];
        $reordered = [];

        foreach ($names as $name) {
            $definition = $this->get($name);
            $ordered[] = $definition;
            $reordered[$definition->entry()->name()] = true;
        }

        foreach (array_values($this->definitions) as $definition) {
            if (!array_key_exists($definition->entry()->name(), $reordered)) {
                $ordered[] = $definition;
            }
        }

        $this->setDefinitions(...$ordered);

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

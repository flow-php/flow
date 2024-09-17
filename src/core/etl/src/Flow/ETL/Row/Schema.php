<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\{InvalidArgumentException, SchemaDefinitionNotFoundException, SchemaDefinitionNotUniqueException};
use Flow\ETL\Row\Schema\{Definition, Matcher\StrictSchemaMatcher, SchemaMatcher};

final class Schema implements \Countable
{
    /**
     * @var array<string, Definition>
     */
    private array $definitions;

    public function __construct(Definition ...$definitions)
    {
        $this->setDefinitions(...$definitions);
    }

    public static function fromArray(array $definitions) : self
    {
        $schema = [];

        foreach ($definitions as $definition) {
            if (!\is_array($definition)) {
                throw new InvalidArgumentException('Schema definition must be an array');
            }

            $schema[] = Definition::fromArray($definition);
        }

        return new self(...$schema);
    }

    public function add(Definition ...$definitions) : self
    {
        $this->setDefinitions(...\array_merge(\array_values($this->definitions), $definitions));

        return $this;
    }

    public function count() : int
    {
        return \count($this->definitions);
    }

    /**
     * @return array<Definition>
     */
    public function definitions() : array
    {
        return $this->definitions;
    }

    /**
     * @return array<Reference>
     *
     *@deprecated use references() : References instead
     */
    public function entries() : array
    {
        return $this->references()->all();
    }

    public function findDefinition(string|Reference $ref) : ?Definition
    {
        if ($ref instanceof Reference) {
            if (!\array_key_exists($ref->name(), $this->definitions)) {
                return null;
            }

            return $this->definitions[$ref->name()];
        }

        if (!\array_key_exists($ref, $this->definitions)) {
            return null;
        }

        return $this->definitions[$ref];
    }

    /**
     * @throws SchemaDefinitionNotFoundException
     */
    public function getDefinition(string|Reference $ref) : Definition
    {
        return $this->findDefinition($ref) ?: throw new SchemaDefinitionNotFoundException((string) $ref);
    }

    /**
     * Gracefully remove entries from schema without throwing an exception if entry does not exist.
     */
    public function gracefulRemove(string|Reference ...$entries) : self
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

    public function keep(string|Reference ...$entries) : self
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
    public function makeNullable() : self
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

    public function matches(self $schema, SchemaMatcher $matcher = new StrictSchemaMatcher()) : bool
    {
        return $matcher->match($this, $schema);
    }

    public function merge(self $schema) : self
    {
        $newDefinitions = $this->definitions;

        if (!$this->count()) {
            return $schema;
        }

        if (!$schema->count()) {
            return $this;
        }

        foreach ($schema->definitions as $entry => $definition) {
            if (!\array_key_exists($definition->entry()->name(), $newDefinitions)) {
                $newDefinitions[$entry] = $definition->makeNullable();
            } else {
                $newDefinitions[$entry] = $newDefinitions[$entry]->merge($definition);
            }
        }

        foreach ($newDefinitions as $entry => $definition) {
            if (!\array_key_exists($definition->entry()->name(), $schema->definitions)) {
                $newDefinitions[$entry] = $definition->makeNullable();
            }
        }

        $this->setDefinitions(...\array_values($newDefinitions));

        return $this;
    }

    public function normalize() : array
    {
        $definitions = [];

        foreach ($this->definitions as $definition) {
            $definitions[] = $definition->normalize();
        }

        return $definitions;
    }

    /**
     * @deprecated use makeNullable instead
     */
    public function nullable() : self
    {
        return $this->makeNullable();
    }

    public function references() : References
    {
        $refs = [];

        foreach ($this->definitions as $definition) {
            $refs[] = $definition->entry();
        }

        return References::init(...$refs);
    }

    public function remove(string|Reference ...$entries) : self
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

    public function rename(string|Reference $entry, string $newName) : self
    {
        $definitions = [];

        if (!$this->findDefinition($entry)) {
            throw new SchemaDefinitionNotFoundException((string) $entry);
        }

        foreach ($this->definitions as $nextDefinition) {
            if ($nextDefinition->entry()->is(Reference::init($entry))) {
                $definitions[] = $nextDefinition->rename($newName);
            } else {
                $definitions[] = $nextDefinition;
            }
        }

        $this->setDefinitions(...$definitions);

        return $this;
    }

    public function replace(string|Reference $entry, Definition $definition) : self
    {
        $definitions = [];

        if (!$this->findDefinition($entry)) {
            throw new SchemaDefinitionNotFoundException((string) $entry);
        }

        foreach ($this->definitions as $nextDefinition) {
            if ($nextDefinition->entry()->is(Reference::init($entry))) {
                $definitions[] = $definition;
            } else {
                $definitions[] = $nextDefinition;
            }
        }

        $this->setDefinitions(...$definitions);

        return $this;
    }

    private function setDefinitions(Definition ...$definitions) : void
    {
        $uniqueDefinitions = [];

        foreach ($definitions as $definition) {
            $uniqueDefinitions[$definition->entry()->name()] = $definition;
        }

        if (\count($uniqueDefinitions) !== \count($definitions)) {
            throw new SchemaDefinitionNotUniqueException(\sprintf('Entry definitions must be unique, given: [%s]', \implode(', ', \array_map(fn (Definition $d) => $d->entry()->name(), $definitions))));
        }

        $this->definitions = $uniqueDefinitions;
    }
}

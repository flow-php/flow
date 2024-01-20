<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Schema\Definition;

final class Schema implements \Countable
{
    /**
     * @var array<string, Definition>
     */
    private readonly array $definitions;

    public function __construct(Definition ...$definitions)
    {
        $uniqueDefinitions = [];

        foreach ($definitions as $definition) {
            $uniqueDefinitions[$definition->entry()->name()] = $definition;
        }

        if (\count($uniqueDefinitions) !== \count($definitions)) {
            throw new InvalidArgumentException(\sprintf('Entry definitions must be unique, given: [%s]', \implode(', ', \array_map(fn (Definition $d) => $d->entry()->name(), $definitions))));
        }

        $this->definitions = $uniqueDefinitions;
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
     */
    public function entries() : array
    {
        $refs = [];

        foreach ($this->definitions as $definition) {
            $refs[] = $definition->entry();
        }

        return $refs;
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
     * @throws InvalidArgumentException
     */
    public function getDefinition(string|Reference $ref) : Definition
    {
        return $this->findDefinition($ref) ?: throw new InvalidArgumentException("There is no definition for \"{$ref}\" in the schema.");
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

//        echo "Before: \n";
//        dj($this->definitions, $schema->definitions);

        foreach ($schema->definitions as $entry => $definition) {
            if (!\array_key_exists($definition->entry()->name(), $newDefinitions)) {
                $newDefinitions[$entry] = $definition->nullable();
            } elseif (!$newDefinitions[$entry]->isEqual($definition)) {
                $newDefinitions[$entry] = $newDefinitions[$entry]->merge($definition);
            }
        }

        foreach ($schema->definitions as $entry => $definition) {
            if (!\array_key_exists($definition->entry()->name(), $newDefinitions)) {
                $newDefinitions[$entry] = $definition->nullable();
            }
        }

//        echo "After: \n";
//        dj($newDefinitions);


        return new self(...\array_values($newDefinitions));
    }

    public function narrow() : self
    {
        $definitions = [];

        foreach ($this->definitions as $definition) {
            $definitions[] = $definition->narrow();
        }

        return new self(...$definitions);
    }

    public function nullable() : self
    {
        $definitions = [];

        foreach ($this->definitions as $definition) {
            if (!$definition->isNullable()) {
                $definitions[] = $definition->nullable();
            } else {
                $definitions[] = $definition;
            }
        }

        return new self(...$definitions);
    }

    public function without(string|Reference ...$entries) : self
    {
        $refs = References::init(...$entries);

        $definitions = [];

        foreach ($this->definitions as $definition) {
            if (!$refs->has($definition->entry())) {
                $definitions[] = $definition;
            }
        }

        return new self(...$definitions);
    }
}

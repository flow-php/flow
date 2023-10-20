<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Schema\Definition;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{definitions: array<string, Definition>}>
 */
final class Schema implements \Countable, Serializable
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

    public function __serialize() : array
    {
        return [
            'definitions' => $this->definitions,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->definitions = $data['definitions'];
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
     * @return array<EntryReference>
     */
    public function entries() : array
    {
        $refs = [];

        foreach ($this->definitions as $definition) {
            $refs[] = $definition->entry();
        }

        return $refs;
    }

    public function findDefinition(string|EntryReference $ref) : ?Definition
    {
        if ($ref instanceof EntryReference) {
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
    public function getDefinition(string|EntryReference $ref) : Definition
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

        foreach ($schema->definitions as $entry => $definition) {
            if (!\array_key_exists($definition->entry()->name(), $newDefinitions)) {
                $newDefinitions[$entry] = $definition->nullable();
            } elseif (!$newDefinitions[$entry]->isEqual($definition)) {
                $newDefinitions[$entry] = $newDefinitions[$entry]->merge($definition);
            }
        }

        return new self(...\array_values($newDefinitions));
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

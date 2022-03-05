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
    private array $definitions;

    public function __construct(Definition ...$definitions)
    {
        $uniqueDefinitions = [];

        foreach ($definitions as $definition) {
            $uniqueDefinitions[$definition->entry()] = $definition;
        }

        if (\count($uniqueDefinitions) !== \count($definitions)) {
            throw new InvalidArgumentException(\sprintf('Entry definitions must be unique, given: [%s]', \implode(', ', \array_map(fn (Definition $d) => $d->entry(), $definitions))));
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
     * @return array<string>
     */
    public function entries() : array
    {
        return \array_keys($this->definitions);
    }

    public function findDefinition(string $entry) : ?Definition
    {
        if (!\array_key_exists($entry, $this->definitions)) {
            return null;
        }

        return $this->definitions[$entry];
    }

    public function getDefinition(string $entry) : ?Definition
    {
        if (!\array_key_exists($entry, $this->definitions)) {
            throw new InvalidArgumentException("There is no definition for \"{$entry}\" in the schema.");
        }

        return $this->definitions[$entry];
    }
}

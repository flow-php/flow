<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Schema\Definition;
use Flow\Serializer\Serializable;

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

    /**
     * @return array{definitions: array<string, Definition>}
     */
    public function __serialize() : array
    {
        return [
            'definitions' => $this->definitions,
        ];
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{definitions: array<string, Definition>} $data
     */
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

    public function isValid(Row $row) : bool
    {
        if (\count($this->definitions) !== $row->entries()->count()) {
            return false;
        }

        foreach ($row->entries()->all() as $entry) {
            $isValid = false;

            foreach ($this->definitions as $definition) {
                if ($definition->matches($entry)) {
                    $isValid = true;

                    break;
                }
            }

            if (!$isValid) {
                return false;
            }
        }

        return true;
    }
}

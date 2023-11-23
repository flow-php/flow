<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\EntryIdFactory;

use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

/**
 * @implements IdFactory<array{hash_name: string, entry_names: array<string>}>
 */
final class HashIdFactory implements IdFactory
{
    /**
     * @var string[]
     */
    private array $entryNames;

    private string $hashName = 'xxh128';

    public function __construct(string ...$entryNames)
    {
        $this->entryNames = $entryNames;
    }

    public function __serialize() : array
    {
        return [
            'entry_names' => $this->entryNames,
            'hash_name' => $this->hashName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryNames = $data['entry_names'];
        $this->hashName = $data['hash_name'];
    }

    public function create(Row $row) : Entry
    {
        return new Row\Entry\StringEntry(
            'id',
            \hash(
                $this->hashName,
                \implode(':', \array_map(fn (string $name) : string => (string) $row->valueOf($name), $this->entryNames))
            )
        );
    }

    public function withHash(string $hashName) : self
    {
        if (!\in_array($hashName, \hash_algos(), true)) {
            throw InvalidArgumentException::because('Unsupported hash algorithm name provided: ' . $hashName . ', did you mean: ' . \implode(', ', \hash_algos()));
        }

        $factory = new self(...$this->entryNames);
        $factory->hashName = $hashName;

        return $factory;
    }
}

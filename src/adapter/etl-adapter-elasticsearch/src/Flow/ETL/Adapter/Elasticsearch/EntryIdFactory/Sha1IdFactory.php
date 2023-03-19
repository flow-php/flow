<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\EntryIdFactory;

use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

/**
 * @implements IdFactory<array{entry_names: array<string>}>
 */
final class Sha1IdFactory implements IdFactory
{
    /**
     * @var string[]
     */
    private array $entryNames;

    public function __construct(string ...$entryNames)
    {
        $this->entryNames = $entryNames;
    }

    public function __serialize() : array
    {
        return [
            'entry_names' => $this->entryNames,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryNames = $data['entry_names'];
    }

    public function create(Row $row) : Entry
    {
        return new Row\Entry\StringEntry(
            'id',
            \sha1(
                /** @phpstan-ignore-next-line */
                \implode(':', \array_map(fn (string $name) : string => (string) $row->valueOf($name), $this->entryNames))
            )
        );
    }
}

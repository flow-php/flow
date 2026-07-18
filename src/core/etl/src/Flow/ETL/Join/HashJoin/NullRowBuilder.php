<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;

use function array_key_exists;

final class NullRowBuilder
{
    /**
     * @var array<string, Entry<mixed>>
     */
    private array $entries = [];

    public function __construct(
        private readonly EntryFactory $entryFactory,
    ) {}

    public function collect(Row $row): void
    {
        foreach ($row->entries()->all() as $entry) {
            $name = $entry->name();

            if (!array_key_exists($name, $this->entries)) {
                $nullable = $entry->definition()->makeNullable();
                $this->entries[$name] = $this->entryFactory->create(
                    $name,
                    null,
                    $nullable->type(),
                    $nullable->metadata(),
                );
            }
        }
    }

    public function row(): Row
    {
        return new Row(Entries::recreate($this->entries));
    }
}

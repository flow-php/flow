<?php

declare(strict_types=1);

namespace Flow\ETL\Filter;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class TrimRowToContainOnly
{
    /**
     * @psalm-var array<int, string>
     */
    private array $names;

    public function __construct(string ...$names)
    {
        $this->names = $names;
    }

    public function __invoke(Row $row) : Row
    {
        return $row->filter(
            fn (Entry $entry) : bool => \in_array($entry->name(), $this->names, true)
        );
    }
}

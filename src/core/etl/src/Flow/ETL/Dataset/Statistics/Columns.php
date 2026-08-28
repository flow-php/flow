<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Statistics;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use InvalidArgumentException;

use function array_key_exists;
use function array_values;
use function sprintf;

final class Columns
{
    /**
     * @var array<string, Column>
     */
    private array $columns = [];

    public function __construct() {}

    /**
     * @param Entry<mixed> $entry
     */
    public function add(Entry $entry): void
    {
        if (!array_key_exists($entry->name(), $this->columns)) {
            $this->columns[$entry->name()] = new Column($entry);

            return;
        }

        $this->columns[$entry->name()]->calculate($entry);
    }

    /**
     * @return array<Column>
     */
    public function all(): array
    {
        return array_values($this->columns);
    }

    public function get(string|Reference $ref): Column
    {
        if ($ref instanceof Reference) {
            $ref = $ref->name();
        }

        if (!array_key_exists($ref, $this->columns)) {
            throw new InvalidArgumentException(sprintf('Column "%s" does not exist.', $ref));
        }

        return $this->columns[$ref];
    }
}

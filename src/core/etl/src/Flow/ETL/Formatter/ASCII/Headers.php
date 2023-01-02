<?php

declare(strict_types=1);

namespace Flow\ETL\Formatter\ASCII;

use Flow\ETL\Rows;

final class Headers implements \Countable
{
    /**
     * @var null|array<string>
     */
    private ?array $names = null;

    public function __construct(private readonly Rows $rows)
    {
    }

    public function count() : int
    {
        return \count($this->names());
    }

    /**
     * @return array<string>
     */
    public function names() : array
    {
        if ($this->names !== null) {
            return $this->names;
        }

        $names = [];

        foreach ($this->rows->entries() as $entries) {
            foreach ($entries->all() as $entry) {
                if (!\in_array($entry->name(), $names, true)) {
                    $names[] = $entry->name();
                }
            }
        }

        $this->names = $names;

        return $this->names;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter;

/**
 * @implements Filter<array{entry_name: string}>
 * @psalm-immutable
 */
final class EntryExists implements Filter
{
    public function __construct(private string $entryName)
    {
    }

    public function __serialize() : array
    {
        return [
            'entry_name' => $this->entryName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryName = $data['entry_name'];
    }

    public function keep(Row $row) : bool
    {
        return $row->entries()->has($this->entryName);
    }
}

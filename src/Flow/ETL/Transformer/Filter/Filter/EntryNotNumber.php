<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter;

/**
 * @psalm-immutable
 */
final class EntryNotNumber implements Filter
{
    private string $entryName;

    /**
     * @param string $entryName
     */
    public function __construct(string $entryName)
    {
        $this->entryName = $entryName;
    }

    /**
     * @return array{entry_name: string}
     */
    public function __serialize() : array
    {
        return [
            'entry_name' => $this->entryName,
        ];
    }

    /**
     * @param array{entry_name: string} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->entryName = $data['entry_name'];
    }

    public function keep(Row $row) : bool
    {
        return !\is_numeric($row->get($this->entryName)->value());
    }
}

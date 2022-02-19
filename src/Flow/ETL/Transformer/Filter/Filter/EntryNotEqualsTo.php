<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter;

/**
 * @psalm-immutable
 */
final class EntryNotEqualsTo implements Filter
{
    private string $entryName;

    /**
     * @var mixed
     */
    private $entryValue;

    /**
     * @param string $entryName
     * @param mixed $entryValue
     */
    public function __construct(string $entryName, $entryValue)
    {
        $this->entryName = $entryName;
        $this->entryValue = $entryValue;
    }

    /**
     * @return array{entry_name: string, entry_value: mixed}
     */
    public function __serialize() : array
    {
        return [
            'entry_name' => $this->entryName,
            'entry_value' => $this->entryValue,
        ];
    }

    /**
     * @param array{entry_name: string, entry_value: mixed} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->entryName = $data['entry_name'];
        $this->entryValue = $data['entry_value'];
    }

    public function keep(Row $row) : bool
    {
        $entry = $row->get($this->entryName);

        if ($entry instanceof Row\Entry\FloatEntry) {
            if (!\is_numeric($this->entryValue)) {
                return false;
            }

            return \bccomp((string) $entry->value(), (string) $this->entryValue, 8) !== 0;
        }

        return $entry->value() !== $this->entryValue;
    }
}

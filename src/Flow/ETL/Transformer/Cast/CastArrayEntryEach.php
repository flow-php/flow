<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast;

use Flow\ETL\Row;
use Flow\ETL\Row\RowConverter;
use Flow\ETL\Row\ValueConverter;

/**
 * @implements RowConverter<array{caster: ValueConverter, array_entry_name: string}>
 * @psalm-immutable
 */
class CastArrayEntryEach implements RowConverter
{
    private string $arrayEntryName;

    private ValueConverter $caster;

    public function __construct(string $arrayEntryName, ValueConverter $caster)
    {
        $this->arrayEntryName = $arrayEntryName;
        $this->caster = $caster;
    }

    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'caster' => $this->caster,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->caster = $data['caster'];
    }

    final public function convert(Row $row) : Row
    {
        if (!$row->entries()->has($this->arrayEntryName)) {
            return $row;
        }

        $entry = $row->entries()->get($this->arrayEntryName);

        if (!$entry instanceof Row\Entry\ArrayEntry) {
            return $row;
        }

        return new Row(
            $row->entries()
                ->remove($entry->name())
                ->add(
                    new Row\Entry\ArrayEntry(
                        $entry->name(),
                        \array_map([$this->caster, 'convert'], $entry->value())
                    )
                )
        );
    }
}

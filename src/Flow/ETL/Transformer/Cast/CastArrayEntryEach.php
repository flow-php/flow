<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast;

use Flow\ETL\Row;
use Flow\ETL\Row\RowConverter;
use Flow\ETL\Row\ValueConverter;

/**
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

    /**
     * @return array{caster: ValueConverter, array_entry_name: string}
     */
    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'caster' => $this->caster,
        ];
    }

    /**
     * @param array{caster: ValueConverter, array_entry_name: string} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
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

        /**
         * @psalm-suppress ImpureFunctionCall
         * @psalm-suppress MissingClosureReturnType
         */
        return new Row(
            $row->entries()
                ->remove($entry->name())
                ->add(
                    new Row\Entry\ArrayEntry(
                        $entry->name(),
                        \array_map(
                            fn ($value) => $this->caster->convert($value),
                            $entry->value()
                        )
                    )
                )
        );
    }
}

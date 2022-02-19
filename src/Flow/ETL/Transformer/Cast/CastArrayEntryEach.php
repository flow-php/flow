<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast;

use Flow\ETL\Row;

/**
 * @psalm-immutable
 */
class CastArrayEntryEach implements CastRow
{
    private string $arrayEntryName;

    private ValueCaster $caster;

    public function __construct(string $arrayEntryName, ValueCaster $caster)
    {
        $this->arrayEntryName = $arrayEntryName;
        $this->caster = $caster;
    }

    /**
     * @return array{caster: ValueCaster, array_entry_name: string}
     */
    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'caster' => $this->caster,
        ];
    }

    /**
     * @param array{caster: ValueCaster, array_entry_name: string} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->caster = $data['caster'];
    }

    final public function cast(Row $row) : Row
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
                            fn ($value) => $this->caster->cast($value),
                            $entry->value()
                        )
                    )
                )
        );
    }
}

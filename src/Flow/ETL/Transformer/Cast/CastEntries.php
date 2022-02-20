<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast;

use Flow\ETL\Row;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Row\RowConverter;

/**
 * @psalm-immutable
 */
class CastEntries implements RowConverter
{
    /**
     * @var array<string>
     */
    private array $entryNames;

    private EntryConverter $caster;

    private bool $nullable;

    /**
     * @param array<string> $entryNames
     * @param EntryConverter $caster
     * @param bool $nullable
     */
    public function __construct(array $entryNames, EntryConverter $caster, bool $nullable = false)
    {
        $this->entryNames = $entryNames;
        $this->nullable = $nullable;
        $this->caster = $caster;
    }

    /**
     * @return array{entry_names: array<string>, nullable: boolean, caster: EntryConverter}
     */
    public function __serialize() : array
    {
        return [
            'entry_names' => $this->entryNames,
            'nullable' => $this->nullable,
            'caster' => $this->caster,
        ];
    }

    /**
     * @param array{entry_names: array<string>, nullable: boolean, caster: EntryConverter} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->entryNames = $data['entry_names'];
        $this->nullable = $data['nullable'];
        $this->caster = $data['caster'];
    }

    final public function convert(Row $row) : Row
    {
        foreach ($this->entryNames as $entryName) {
            if ($row->entries()->has($entryName)) {
                $entry = $row->entries()->get($entryName);

                if ($this->nullable && $entry instanceof Row\Entry\NullEntry) {
                    continue;
                }

                $row = new Row($row->entries()->set($this->caster->convert($entry)));
            }
        }

        return $row;
    }
}

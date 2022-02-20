<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToArrayCaster;

/**
 * @psalm-immutable
 */
final class AnyToArrayEntryCaster implements EntryConverter
{
    private AnyToArrayCaster $valueCaster;

    public function __construct()
    {
        $this->valueCaster = new AnyToArrayCaster();
    }

    /**
     * @return array{value_caster: AnyToArrayCaster}
     */
    public function __serialize() : array
    {
        return [
            'value_caster' => $this->valueCaster,
        ];
    }

    /**
     * @param array{value_caster: AnyToArrayCaster} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->valueCaster = $data['value_caster'];
    }

    public function convert(Entry $entry) : Entry
    {
        return new ArrayEntry(
            $entry->name(),
            $this->valueCaster->convert($entry->value())
        );
    }
}

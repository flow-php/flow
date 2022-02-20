<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToFloatCaster;

/**
 * @psalm-immutable
 */
final class AnyToFloatEntryCaster implements EntryConverter
{
    private AnyToFloatCaster $valueCaster;

    public function __construct()
    {
        $this->valueCaster = new AnyToFloatCaster();
    }

    /**
     * @return array{value_caster: AnyToFloatCaster}
     */
    public function __serialize() : array
    {
        return [
            'value_caster' => $this->valueCaster,
        ];
    }

    /**
     * @param array{value_caster: AnyToFloatCaster} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->valueCaster = $data['value_caster'];
    }

    public function convert(Entry $entry) : Entry
    {
        return new FloatEntry(
            $entry->name(),
            $this->valueCaster->convert($entry->value())
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\EntryConverter;
use Flow\ETL\Transformer\Cast\ValueCaster\AnyToIntegerCaster;

/**
 * @psalm-immutable
 */
final class AnyToIntegerEntryCaster implements EntryConverter
{
    private AnyToIntegerCaster $valueCaster;

    public function __construct()
    {
        $this->valueCaster = new AnyToIntegerCaster();
    }

    /**
     * @return array{value_caster: AnyToIntegerCaster}
     */
    public function __serialize() : array
    {
        return [
            'value_caster' => $this->valueCaster,
        ];
    }

    /**
     * @param array{value_caster: AnyToIntegerCaster} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->valueCaster = $data['value_caster'];
    }

    public function convert(Entry $entry) : Entry
    {
        return new IntegerEntry(
            $entry->name(),
            $this->valueCaster->convert($entry->value())
        );
    }
}

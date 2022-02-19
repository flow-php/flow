<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Cast\EntryCaster;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Transformer\Cast\EntryCaster;
use Flow\ETL\Transformer\Cast\ValueCaster;

/**
 * @psalm-immutable
 */
final class AnyToIntegerEntryCaster implements EntryCaster
{
    private ValueCaster\AnyToIntegerCaster $valueCaster;

    public function __construct()
    {
        $this->valueCaster = new ValueCaster\AnyToIntegerCaster();
    }

    /**
     * @return array{value_caster: ValueCaster\AnyToIntegerCaster}
     */
    public function __serialize() : array
    {
        return [
            'value_caster' => $this->valueCaster,
        ];
    }

    /**
     * @param array{value_caster: ValueCaster\AnyToIntegerCaster} $data
     *
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->valueCaster = $data['value_caster'];
    }

    public function cast(Entry $entry) : Entry
    {
        return new IntegerEntry(
            $entry->name(),
            $this->valueCaster->cast($entry->value())
        );
    }
}

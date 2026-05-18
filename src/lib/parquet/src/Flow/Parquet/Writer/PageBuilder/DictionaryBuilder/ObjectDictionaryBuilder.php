<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder\DictionaryBuilder;

use DateTimeInterface;
use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Writer\PageBuilder\Dictionary;

use function serialize;

final class ObjectDictionaryBuilder
{
    public function build(WriteFlatColumnValues $data): Dictionary
    {
        $dictionary = [];
        $indices = [];
        $valueToIndex = [];
        $dictionarySize = 0;

        foreach ($data->values() as $value) {
            if ($value === null) {
                continue;
            }

            if ($value instanceof DateTimeInterface) {
                $hash = $value->format('U u');
            } else {
                $hash = serialize($value);
            }

            if (!isset($valueToIndex[$hash])) {
                $dictionary[] = $value;
                $valueToIndex[$hash] = $dictionarySize;
                $dictionarySize++;
            }

            $indices[] = $valueToIndex[$hash];
        }

        return new Dictionary($dictionary, $indices);
    }
}

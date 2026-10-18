<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder\DictionaryBuilder;

use DateTimeInterface;
use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Writer\PageBuilder\Dictionary;

use function count;
use function serialize;

final class ObjectDictionaryBuilder
{
    public function build(WriteFlatColumnValues $data, Dictionary $dictionary = new Dictionary([], [])): Dictionary
    {
        $entries = $dictionary->dictionary;
        $indices = [];
        $valueToIndex = $dictionary->keys;
        $dictionarySize = count($entries);

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
                $entries[] = $value;
                $valueToIndex[$hash] = $dictionarySize;
                $dictionarySize++;
            }

            $indices[] = $valueToIndex[$hash];
        }

        return new Dictionary($entries, $indices, $valueToIndex);
    }
}

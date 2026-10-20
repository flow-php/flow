<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder\DictionaryBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Writer\PageBuilder\Dictionary;

use function count;
use function pack;

final class FloatDictionaryBuilder
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

            $hash = pack('E', $value);

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

<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder\DictionaryBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Writer\PageBuilder\Dictionary;

use function pack;

final class FloatDictionaryBuilder
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

            $hash = pack('E', $value);

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

<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder\DictionaryBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Writer\PageBuilder\Dictionary;

final class ScalarDictionaryBuilder
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

            if (\is_float($value)) {
                $key = \pack('E', $value);
            } elseif (\is_int($value) || \is_string($value)) {
                $key = $value;
            } elseif (\is_bool($value)) {
                $key = $value ? 1 : 0;
            } else {
                $key = \serialize($value);
            }

            if (!isset($valueToIndex[$key])) {
                $dictionary[] = $value;
                $valueToIndex[$key] = $dictionarySize;
                $dictionarySize++;
            }

            $indices[] = $valueToIndex[$key];
        }

        return new Dictionary($dictionary, $indices);
    }
}

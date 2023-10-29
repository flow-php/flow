<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryBuilder;

use function Flow\Parquet\array_flatten;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\Dictionary;

final class ScalarDictionaryBuilder
{
    public function build(array $rows) : Dictionary
    {
        $dictionary = [];
        $indices = [];
        $valueToIndex = [];
        $dictionarySize = 0;

        foreach (array_flatten($rows) as $value) {
            if ($value === null) {
                continue;
            }

            if (!isset($valueToIndex[$value])) {
                $dictionary[] = $value;
                $valueToIndex[$value] = $dictionarySize;
                $dictionarySize++;
            }

            $indices[] = $valueToIndex[$value];
        }

        foreach ($dictionary as $index => $value) {
            $dictionary[$index] = $value;
        }

        return new Dictionary($dictionary, $indices);
    }
}

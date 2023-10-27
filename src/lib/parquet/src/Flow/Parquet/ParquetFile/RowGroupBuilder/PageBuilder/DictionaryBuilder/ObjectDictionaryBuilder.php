<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryBuilder;

use function Flow\Parquet\array_flatten;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\Dictionary;

final class ObjectDictionaryBuilder
{
    public function build(array $rows) : Dictionary
    {
        $dictionary = [];
        $indices = [];
        $valueToIndex = [];
        $dictionarySize = 0;

        foreach (array_flatten($rows) as $value) {
            $hash = \serialize($value);

            if (!isset($valueToIndex[$hash])) {
                $dictionary[] = $hash;
                $valueToIndex[$hash] = $dictionarySize;
                $dictionarySize++;
            }

            $indices[] = $valueToIndex[$hash];
        }

        foreach ($dictionary as $index => $value) {
            $dictionary[$index] = @\unserialize($value, ['allowed_classes' => [\DateTimeImmutable::class, \DateInterval::class]]);
        }

        return new Dictionary($dictionary, $indices);
    }
}

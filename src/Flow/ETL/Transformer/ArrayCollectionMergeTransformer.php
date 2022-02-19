<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @psalm-immutable
 */
final class ArrayCollectionMergeTransformer implements Transformer
{
    private string $arrayEntryName;

    private string $newEntryName;

    /**
     * @param string $arrayEntryName
     * @param string $newEntryName
     */
    public function __construct(string $arrayEntryName, string $newEntryName = 'element')
    {
        $this->arrayEntryName = $arrayEntryName;
        $this->newEntryName = $newEntryName;
    }

    /**
     * @return array{array_entry_name: string, new_entry_name: string}
     */
    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'new_entry_name' => $this->newEntryName,
        ];
    }

    /**
     * @param array{array_entry_name: string, new_entry_name: string} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->newEntryName = $data['new_entry_name'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         *
         * @throws \Flow\ArrayDot\Exception\InvalidPathException
         */
        $transformer = function (Row $row) : Row {
            $arrayEntry = $row->get($this->arrayEntryName);

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                $entryClass = \get_class($arrayEntry);

                throw new RuntimeException("{$this->arrayEntryName} is not ArrayEntry but {$entryClass}");
            }

            foreach ($arrayEntry->value() as $index => $element) {
                if (!\is_array($element)) {
                    $type = \gettype($element);

                    throw new RuntimeException("{$this->arrayEntryName}, must be an array of arrays, instead element at position \"{$index}\" is {$type}");
                }
            }

            /** @psalm-suppress MixedArgument */
            return $row->add(
                new Row\Entry\ArrayEntry(
                    $this->newEntryName,
                    /** @phpstan-ignore-next-line  */
                    \array_merge(...\array_values($arrayEntry->value()))
                )
            );
        };

        return $rows->map($transformer);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{array_entry_name: string, new_entry_name: string}>
 *
 * @psalm-immutable
 */
final class ArrayCollectionMergeTransformer implements Transformer
{
    public function __construct(
        private readonly string $arrayEntryName,
        private readonly string $newEntryName = 'element'
    ) {
    }

    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'new_entry_name' => $this->newEntryName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->newEntryName = $data['new_entry_name'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         *
         * @throws \Flow\ArrayDot\Exception\InvalidPathException
         */
        $transformer = function (Row $row) : Row {
            $arrayEntry = $row->get($this->arrayEntryName);

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                $entryClass = $arrayEntry::class;

                throw new RuntimeException("{$this->arrayEntryName} is not ArrayEntry but {$entryClass}");
            }

            foreach ($arrayEntry->value() as $index => $element) {
                if (!\is_array($element)) {
                    $type = \gettype($element);

                    throw new RuntimeException("{$this->arrayEntryName}, must be an array of arrays, instead element at position \"{$index}\" is {$type}");
                }
            }

            /** @psalm-suppress MixedArgument */
            return $row->set(
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

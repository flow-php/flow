<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ArrayDot\array_dot_get;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{keys: array<string>, array_entry_name: string, new_entry_name: string, index: string}>
 *
 * @psalm-immutable
 */
final class ArrayCollectionGetTransformer implements Transformer
{
    private string $index = '*';

    /**
     * @param array<string> $keys
     */
    public function __construct(
        private readonly array $keys,
        private readonly string $arrayEntryName,
        private readonly string $newEntryName = 'element'
    ) {
    }

    /**
     * @param array<string> $keys
     */
    public static function fromFirst(array $keys, string $arrayEntryName, string $newEntryName = 'element') : self
    {
        $transformer = new self($keys, $arrayEntryName, $newEntryName);
        $transformer->index = '0';

        return $transformer;
    }

    public function __serialize() : array
    {
        return [
            'keys' => $this->keys,
            'array_entry_name' => $this->arrayEntryName,
            'new_entry_name' => $this->newEntryName,
            'index' => $this->index,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->keys = $data['keys'];
        $this->arrayEntryName = $data['array_entry_name'];
        $this->newEntryName = $data['new_entry_name'];
        $this->index = $data['index'];
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

            $path = \sprintf("{$this->index}.{%s}", \implode(',', \array_map(fn (string $entryName) : string => '?' . $entryName, $this->keys)));

            try {
                $array = ($this->index === '0') ? \array_values($arrayEntry->value()) : $arrayEntry->value();

                $extractedValues = array_dot_get($array, $path);
            } catch (InvalidPathException) {
                throw new RuntimeException("{$this->arrayEntryName}, must be an array of array (collection of arrays) but it seems to be a regular array.");
            }

            if (!\is_array($extractedValues)) {
                $type = \gettype($extractedValues);

                throw new RuntimeException("Extracted value from path \"{$path}\" is not array but {$type}");
            }

            return $row->set(
                new Row\Entry\ArrayEntry(
                    $this->newEntryName,
                    $extractedValues
                )
            );
        };

        return $rows->map($transformer);
    }
}

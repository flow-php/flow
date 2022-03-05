<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ArrayDot\array_dot_get;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{keys: array<string>, array_entry_name: string, new_entry_name: string, index: string}>
 * @psalm-immutable
 */
final class ArrayCollectionGetTransformer implements Transformer
{
    private string $arrayEntryName;

    private string $index;

    /**
     * @var array<string>
     */
    private array $keys;

    private string $newEntryName;

    /**
     * @param array<string> $keys
     * @param string $arrayEntryName
     * @param string $newEntryName
     */
    public function __construct(array $keys, string $arrayEntryName, string $newEntryName = 'element')
    {
        $this->keys = $keys;
        $this->arrayEntryName = $arrayEntryName;
        $this->newEntryName = $newEntryName;
        $this->index = '*';
    }

    /**
     * @param array<string> $keys
     * @param string $arrayEntryName
     * @param string $newEntryName
     *
     * @return static
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

            $path = \sprintf("{$this->index}.{%s}", \implode(',', \array_map(fn (string $entryName) : string => '?' . $entryName, $this->keys)));

            try {
                $array = ($this->index === '0') ? \array_values($arrayEntry->value()) : $arrayEntry->value();

                $extractedValues = array_dot_get($array, $path);
            } catch (InvalidPathException $e) {
                throw new RuntimeException("{$this->arrayEntryName}, must be an array of array (collection of arrays) but it seems to be a regular array.");
            }

            if (!\is_array($extractedValues)) {
                $type = \gettype($extractedValues);

                throw new RuntimeException("Extracted value from path \"{$path}\" is not array but {$type}");
            }

            return $row->add(
                new Row\Entry\ArrayEntry(
                    $this->newEntryName,
                    $extractedValues
                )
            );
        };

        return $rows->map($transformer);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{array_entry_name: string}>
 * @psalm-immutable
 */
final class ArrayReverseTransformer implements Transformer
{
    public function __construct(private readonly string $arrayEntryName)
    {
    }

    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
    }

    public function transform(Rows $rows) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            if (!$row->entries()->has($this->arrayEntryName)) {
                throw new RuntimeException("\"{$this->arrayEntryName}\" not found");
            }

            if (!$row->entries()->get($this->arrayEntryName) instanceof Row\Entry\ArrayEntry) {
                throw new RuntimeException("\"{$this->arrayEntryName}\" is not ArrayEntry");
            }

            $arrayEntry = $row->get($this->arrayEntryName);

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                throw new InvalidArgumentException("Entry {$this->arrayEntryName} is not a ArrayEntry");
            }

            return $row->set(new Row\Entry\ArrayEntry(
                $arrayEntry->name(),
                \array_reverse($arrayEntry->value())
            ));
        };

        return $rows->map($transformer);
    }
}

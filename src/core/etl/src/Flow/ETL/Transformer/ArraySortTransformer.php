<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{array_entry_name: string, sorting_flag: int}>
 */
final class ArraySortTransformer implements Transformer
{
    public function __construct(
        private readonly string $arrayEntryName,
        private readonly int $sortingFlag = \SORT_REGULAR
    ) {
    }

    public function __serialize() : array
    {
        return [
            'array_entry_name' => $this->arrayEntryName,
            'sorting_flag' => $this->sortingFlag,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayEntryName = $data['array_entry_name'];
        $this->sortingFlag = $data['sorting_flag'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = function (Row $row) : Row {
            if (!$row->entries()->has($this->arrayEntryName)) {
                throw new RuntimeException("\"{$this->arrayEntryName}\" not found");
            }

            if (!$row->entries()->get($this->arrayEntryName) instanceof Row\Entry\ArrayEntry) {
                throw new RuntimeException("\"{$this->arrayEntryName}\" is not ArrayEntry");
            }

            $arrayEntry = $row->get($this->arrayEntryName);

            /** @var array<mixed> $array */
            $array = $arrayEntry->value();
            \sort($array, $this->sortingFlag);

            return $row->set(new Row\Entry\ArrayEntry(
                $arrayEntry->name(),
                $array
            ));
        };

        return $rows->map($transformer);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{array_entry: string, values: array}>
 */
final class ArrayPushTransformer implements Transformer
{
    /**
     * @param string $arrayEntry
     * @param array<mixed> $values
     */
    public function __construct(
        private readonly string $arrayEntry,
        private readonly array $values = []
    ) {
    }

    public function __serialize() : array
    {
        return [
            'array_entry' => $this->arrayEntry,
            'values' => $this->values,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->arrayEntry = $data['array_entry'];
        $this->values = $data['values'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            if (!$row->entries()->has($this->arrayEntry)) {
                $arrayEntry = Entry::array($this->arrayEntry, []);
            } else {
                $arrayEntry = $row->entries()->get($this->arrayEntry);
            }

            if (!$arrayEntry instanceof Row\Entry\ArrayEntry) {
                throw new RuntimeException("\"{$this->arrayEntry}\" is not ArrayEntry");
            }

            return $row->set(
                Entry::array($this->arrayEntry, \array_merge($arrayEntry->value(), $this->values))
            );
        };

        return $rows->map($transformer);
    }
}

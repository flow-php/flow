<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{string_entry_names: array<string>, glue: string, new_entry_name: string}>
 */
final class StringConcatTransformer implements Transformer
{
    /**
     * @param array<string> $stringEntryNames
     */
    public function __construct(
        private readonly array $stringEntryNames,
        private readonly string $glue = ' ',
        private readonly string $newEntryName = 'element'
    ) {
    }

    public function __serialize() : array
    {
        return [
            'string_entry_names' => $this->stringEntryNames,
            'glue' => $this->glue,
            'new_entry_name' => $this->newEntryName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->stringEntryNames = $data['string_entry_names'];
        $this->glue = $data['glue'];
        $this->newEntryName = $data['new_entry_name'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = function (Row $row) : Row {
            /** @psalm-var pure-callable(Row\Entry) : bool $filter */
            $filter = fn (Row\Entry $entry) : bool => \in_array($entry->name(), $this->stringEntryNames, true) && $entry instanceof Row\Entry\StringEntry;
            $entries = $row->filter($filter)->entries();
            /** @var array<string> $values */
            $values = [];

            foreach ($entries->all() as $entry) {
                $values[] = $entry->toString();
            }

            return $row->set(
                new Row\Entry\StringEntry(
                    $this->newEntryName,
                    \implode($this->glue, $values)
                )
            );
        };

        return $rows->map($transformer);
    }
}

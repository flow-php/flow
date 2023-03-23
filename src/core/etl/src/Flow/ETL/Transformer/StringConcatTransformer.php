<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{refs: References, glue: string, new_entry_name: string}>
 */
final class StringConcatTransformer implements Transformer
{
    private readonly References $refs;

    /**
     * @param array<Reference|string> $refs
     */
    public function __construct(
        array $refs,
        private readonly string $glue = ' ',
        private readonly string $newEntryName = 'element'
    ) {
        $this->refs = References::init(...$refs);
    }

    public function __serialize() : array
    {
        return [
            'refs' => $this->refs,
            'glue' => $this->glue,
            'new_entry_name' => $this->newEntryName,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->refs = $data['refs'];
        $this->glue = $data['glue'];
        $this->newEntryName = $data['new_entry_name'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = function (Row $row) : Row {
            $filter = fn (Row\Entry $entry) : bool => \in_array(
                $entry->name(),
                \array_map(static fn (EntryReference $r) : string => $r->name(), $this->refs->all()),
                true
            ) && $entry instanceof Row\Entry\StringEntry;

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

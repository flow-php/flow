<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DSL\Entry as EntryDSL;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{names: array<string>}>
 * @psalm-immutable
 */
final class KeepEntriesTransformer implements Transformer
{
    /**
     * @var string[]
     */
    private readonly array $names;

    public function __construct(string ...$names)
    {
        $this->names = $names;
    }

    public function __serialize() : array
    {
        return [
            'names' => $this->names,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->names = $data['names'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /** @psalm-var pure-callable(Row) : Row $transformer */
        $transformer = function (Row $row) : Row {
            $allEntries = $row->entries()->map(fn (Entry $entry) : string => $entry->name());
            $removeEntries = \array_diff($allEntries, $this->names);

            $newEntries = $row->remove(...$removeEntries);

            foreach ($this->names as $keepEntryName) {
                if (!$newEntries->entries()->has($keepEntryName)) {
                    $newEntries = $newEntries->add(EntryDSL::null($keepEntryName));
                }
            }

            return $newEntries;
        };

        return $rows->map($transformer);
    }
}

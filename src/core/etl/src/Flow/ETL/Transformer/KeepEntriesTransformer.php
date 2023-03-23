<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\DSL\Entry as EntryDSL;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{refs: References}>
 */
final class KeepEntriesTransformer implements Transformer
{
    private readonly References $refs;

    public function __construct(string|Reference ...$refs)
    {
        $this->refs = References::init(...$refs);
    }

    public function __serialize() : array
    {
        return [
            'refs' => $this->refs,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->refs = $data['refs'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = function (Row $row) : Row {
            $allEntries = $row->entries()->map(fn (Entry $entry) : string => $entry->name());
            $removeEntries = \array_diff(
                $allEntries,
                \array_map(static fn (EntryReference $r) : string => $r->name(), $this->refs->all())
            );

            $newEntries = $row->remove(...$removeEntries);

            foreach ($this->refs as $keepEntryName) {
                if (!$newEntries->entries()->has($keepEntryName)) {
                    $newEntries = $newEntries->add(EntryDSL::null($keepEntryName->name()));
                }
            }

            return $newEntries;
        };

        return $rows->map($transformer);
    }
}

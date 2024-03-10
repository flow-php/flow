<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ETL\DSL\null_entry;
use Flow\ETL\Row\{Entry, Reference, References};
use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final class KeepEntriesTransformer implements Transformer
{
    private readonly References $refs;

    public function __construct(string|Reference ...$refs)
    {
        $this->refs = References::init(...$refs);
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = function (Row $row) : Row {
            $allEntries = $row->entries()->map(fn (Entry $entry) : string => $entry->name());
            $removeEntries = \array_diff(
                $allEntries,
                \array_map(static fn (Reference $r) : string => $r->name(), $this->refs->all())
            );

            $newEntries = $row->remove(...$removeEntries);

            foreach ($this->refs as $keepEntryName) {
                if (!$newEntries->entries()->has($keepEntryName)) {
                    $newEntries = $newEntries->add(null_entry($keepEntryName->name()));
                }
            }

            return $newEntries;
        };

        return $rows->map($transformer);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ETL\DSL\{row, rows, str_entry};
use Flow\ETL\Row\{Reference, References};
use Flow\ETL\{FlowContext, Rows, Transformer};

final readonly class SelectEntriesTransformer implements Transformer
{
    private References $refs;

    public function __construct(string|Reference ...$refs)
    {
        $this->refs = References::init(...$refs);
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $newRows = [];

        foreach ($rows as $row) {
            $newRowEntries = [];

            foreach ($this->refs as $ref) {
                try {
                    $newRowEntries[] = $row->get($ref);
                } catch (\Exception) {
                    $newRowEntries[] = str_entry($ref->name(), null);
                }
            }
            $newRows[] = row(...$newRowEntries);
        }

        return rows(...$newRows);
    }
}

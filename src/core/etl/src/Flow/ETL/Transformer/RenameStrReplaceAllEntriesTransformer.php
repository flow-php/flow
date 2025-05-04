<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer, Transformer\Rename\RenameReplaceEntryStrategy};

/**
 * @deprecated Use `DataFrame::renameEach()` and `RenameReplaceStrategy`
 */
final readonly class RenameStrReplaceAllEntriesTransformer implements Transformer
{
    private RenameReplaceEntryStrategy $transformer;

    public function __construct(
        private string $search,
        private string $replace,
    ) {
        $this->transformer = new RenameReplaceEntryStrategy($this->search, $this->replace);
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) use ($context) : Row {
            foreach ($row->entries()->all() as $entry) {
                $row = $this->transformer->rename($row, $entry, $context);
            }

            return $row;
        });
    }
}

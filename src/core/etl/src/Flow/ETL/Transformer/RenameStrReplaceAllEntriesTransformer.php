<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{search: string, replace: string}>
 */
final class RenameStrReplaceAllEntriesTransformer implements Transformer
{
    public function __construct(
        private readonly string $search,
        private readonly string $replace
    )
    {
    }

    public function __serialize() : array
    {
        return [
            'search' => $this->search,
            'replace' => $this->replace,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->replace = $data['replace'];
        $this->search = $data['search'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) : Row {
            foreach ($row->entries()->all() as $entry) {
                $row = $row->rename($entry->name(), \str_replace($this->search, $this->replace, $entry->name()));
            }

            return $row;
        });
    }
}

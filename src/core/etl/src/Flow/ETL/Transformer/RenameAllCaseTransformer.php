<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final class RenameAllCaseTransformer implements Transformer
{
    public function __construct(
        private readonly bool $upper = false,
        private readonly bool $lower = false,
        private readonly bool $ucfirst = false,
        private readonly bool $ucwords = false
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) : Row {
            foreach ($row->entries()->all() as $entry) {
                if ($this->upper) {
                    $row = $row->rename($entry->name(), \strtoupper($entry->name()));
                }

                if ($this->lower) {
                    $row = $row->rename($entry->name(), \strtolower($entry->name()));
                }

                if ($this->ucfirst) {
                    $row = $row->rename($entry->name(), \ucfirst($entry->name()));
                }

                if ($this->ucwords) {
                    $row = $row->rename($entry->name(), \ucwords($entry->name()));
                }
            }

            return $row;
        });
    }
}

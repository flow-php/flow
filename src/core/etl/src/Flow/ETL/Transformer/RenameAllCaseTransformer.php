<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, String\StringStyles, Transformer, Transformer\Rename\RenameCaseEntryStrategy};

/**
 * @deprecated Use `DataFrame::renameEach()` and `RenameCaseTransformer`
 */
final class RenameAllCaseTransformer implements Transformer
{
    private RenameCaseEntryStrategy $transformer;

    public function __construct(
        bool $upper = false,
        bool $lower = false,
        bool $ucfirst = false,
        bool $ucwords = false,
    ) {
        if ($upper) {
            $this->transformer = new RenameCaseEntryStrategy(StringStyles::UPPER);
        }

        if ($lower) {
            $this->transformer = new RenameCaseEntryStrategy(StringStyles::LOWER);
        }

        if ($ucfirst) {
            $this->transformer = new RenameCaseEntryStrategy(StringStyles::UCFIRST);
        }

        if ($ucwords) {
            $this->transformer = new RenameCaseEntryStrategy(StringStyles::UCWORDS);
        }
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

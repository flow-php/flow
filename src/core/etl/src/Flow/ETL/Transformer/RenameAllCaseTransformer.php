<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext,
    Row,
    Rows,
    Transformer,
    Transformer\Rename\RenameCaseEntryStrategy,
    Transformer\Rename\Style};

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
            $this->transformer = new RenameCaseEntryStrategy(Style::UPPER);
        }

        if ($lower) {
            $this->transformer = new RenameCaseEntryStrategy(Style::LOWER);
        }

        if ($ucfirst) {
            $this->transformer = new RenameCaseEntryStrategy(Style::UCFIRST);
        }

        if ($ucwords) {
            $this->transformer = new RenameCaseEntryStrategy(Style::UCWORDS);
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

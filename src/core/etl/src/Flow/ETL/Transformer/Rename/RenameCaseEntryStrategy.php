<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\{FlowContext,
    Function\StyleConverter\StringStyles as OldStringStyles,
    Row,
    Row\Entry,
    String\StringStyles};

final class RenameCaseEntryStrategy implements RenameEntryStrategy
{
    private StringStyles $style;

    public function __construct(
        OldStringStyles|StringStyles $style,
    ) {
        if ($style instanceof OldStringStyles) {
            $this->style = StringStyles::fromString($style->value);
        } else {
            $this->style = $style;
        }
    }

    public function rename(Row $row, Entry $entry, FlowContext $context) : Row
    {
        return $row->rename($entry->name(), $this->style->convert($entry->name()));
    }
}

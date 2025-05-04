<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\{FlowContext, Row, Row\Entry};

final readonly class RenameTransliterateEntryStrategy implements RenameEntryStrategy
{
    public function __construct(
        private string $transliterator,
    ) {
    }

    public function rename(Row $row, Entry $entry, FlowContext $context) : Row
    {
        return $row->rename($entry->name(), $this->transliterate($entry->name()));
    }

    private function transliterate(string $string) : string
    {
        if (\function_exists('transliterator_transliterate')) {
            return (string) \transliterator_transliterate($this->transliterator, $string);
        }

        return $string;
    }
}

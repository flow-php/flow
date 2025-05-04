<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\{FlowContext, Row, Row\Entry};

final readonly class RenameCaseEntryStrategy implements RenameEntryStrategy
{
    public function __construct(
        private Style $style,
    ) {
    }

    public function rename(Row $row, Entry $entry, FlowContext $context) : Row
    {
        return match ($this->style) {
            Style::LOWER => $row->rename($entry->name(), \mb_strtolower($entry->name())),
            Style::UPPER => $row->rename($entry->name(), \mb_strtoupper($entry->name())),
            Style::UCFIRST => $row->rename($entry->name(), $this->ucFirst($entry->name())),
            Style::UCWORDS => $row->rename($entry->name(), $this->ucWords($entry->name())),
        };
    }

    private function ucFirst(string $string) : string
    {
        // Available from PHP 8.4+
        if (\function_exists('mb_ucfirst')) {
            return \mb_ucfirst($string);
        }

        $encoding = \mb_internal_encoding();

        return \mb_strtoupper(\mb_substr($string, 0, 1, $encoding), $encoding) . \mb_substr($string, 1, null, $encoding);
    }

    private function ucWords(string $string) : string
    {
        $result = '';
        $previousCharacter = ' ';

        $encoding = \mb_internal_encoding();

        for ($i = 0, $length = \mb_strlen($string, $encoding); $i < $length; $i++) {
            $currentCharacter = \mb_substr($string, $i, 1, $encoding);

            if (' ' === $previousCharacter) {
                $currentCharacter = \mb_strtoupper($currentCharacter, $encoding);
            }

            $result .= $currentCharacter;
            $previousCharacter = $currentCharacter;
        }

        return $result;
    }
}

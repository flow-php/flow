<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer, Transformer\StyleConverter\RenameStrategy};

final readonly class RenameEachTransformer implements Transformer
{
    public function __construct(
        private RenameStrategy $strategy,
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) : Row {
            foreach ($row->entries()->all() as $entry) {
                $row = match ($this->strategy) {
                    RenameStrategy::LOWER => $row->rename($entry->name(), \mb_strtolower($entry->name())),
                    RenameStrategy::UPPER => $row->rename($entry->name(), \mb_strtoupper($entry->name())),
                    RenameStrategy::UCFIRST => $row->rename($entry->name(), $this->ucFirst($entry->name())),
                    RenameStrategy::UCWORDS => $row->rename($entry->name(), $this->ucWords($entry->name())),
                    RenameStrategy::TRANSLITERATE => $row->rename($entry->name(), $this->transliterate($entry->name())),
                };
            }

            return $row;
        });
    }

    private function transliterate(string $string) : string
    {
        if (\function_exists('transliterator_transliterate')) {
            return (string) \transliterator_transliterate('Any-Latin; Latin-ASCII; Lower()', $string);
        }

        return $string;
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

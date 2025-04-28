<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final readonly class RenameAllCaseTransformer implements Transformer
{
    public function __construct(
        private bool $upper = false,
        private bool $lower = false,
        private bool $ucfirst = false,
        private bool $ucwords = false,
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) : Row {
            foreach ($row->entries()->all() as $entry) {
                if ($this->upper) {
                    $row = $row->rename($entry->name(), \mb_strtoupper($entry->name()));
                }

                if ($this->lower) {
                    $row = $row->rename($entry->name(), \mb_strtolower($entry->name()));
                }

                if ($this->ucfirst) {
                    $row = $row->rename($entry->name(), $this->ucFirst($entry->name()));
                }

                if ($this->ucwords) {
                    $row = $row->rename($entry->name(), $this->ucWords($entry->name()));
                }
            }

            return $row;
        });
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

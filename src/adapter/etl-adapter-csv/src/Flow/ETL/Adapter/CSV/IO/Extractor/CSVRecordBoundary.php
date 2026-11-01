<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Extractor;

use function preg_match;
use function preg_quote;
use function str_contains;
use function str_replace;

/**
 * Enclosure opens only at a field start (as fgetcsv); possessive quantifiers keep it linear on large buffers.
 */
final readonly class CSVRecordBoundary
{
    private string $pattern;

    private CSVEnclosureScan $scan;

    public function __construct(
        private string $enclosure,
        string $separator = ',',
        string $escape = '\\',
    ) {
        $quotedSeparator = preg_quote($separator, '/');
        $quotedEnclosure = preg_quote($enclosure, '/');
        $quotedEscape = preg_quote($escape, '/');

        $blanks = '[' . preg_quote(str_replace($separator, '', CSVEnclosureScan::BLANKS), '/') . ']*+';
        $escapedByte = $escape === '' || $escape === $enclosure ? '' : $quotedEscape . '.|';
        $plainBytes = '[^' . $quotedEnclosure . ($escapedByte === '' ? '' : $quotedEscape) . ']++';
        $enclosedBytes = '(?:' . $plainBytes . '|' . $escapedByte . $quotedEnclosure . $quotedEnclosure . ')*+';
        $enclosedField = $quotedEnclosure . $enclosedBytes . $quotedEnclosure . '[^' . $quotedSeparator . ']*+';
        $unenclosedField = '(?!' . $quotedEnclosure . ')[^' . $quotedSeparator . ']*+';
        $fieldEnd = '(?:' . $quotedSeparator . '|\z)';

        $this->pattern =
            '/\A(?:' . $blanks . '(?:' . $enclosedField . '|' . $unenclosedField . ')' . $fieldEnd . ')*+\z/s';
        $this->scan = new CSVEnclosureScan($separator, $enclosure, $escape);
    }

    public function isComplete(string $buffer): bool
    {
        if (!str_contains($buffer, $this->enclosure)) {
            return true;
        }

        $matched = preg_match($this->pattern, $buffer);

        // PCRE gives up at about 142k fields in one record; reading that as "incomplete" would glue every following line
        return $matched === false ? $this->scan->endsOutsideAnEnclosure($buffer) : $matched === 1;
    }
}

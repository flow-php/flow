<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function str_replace;
use function strcspn;
use function strlen;
use function strpos;
use function strspn;

/**
 * CSVRecordBoundary's rules as a byte walk, for the buffers PCRE gives up on.
 */
final readonly class CSVEnclosureScan
{
    /**
     * isspace() in the C locale, which is what fgetcsv skips before an opening enclosure.
     */
    public const string BLANKS = " \t\n\v\f\r";

    private string $blanks;

    private string $specials;

    public function __construct(
        private string $separator,
        private string $enclosure,
        string $escape,
    ) {
        $this->blanks = str_replace($separator, '', self::BLANKS);
        $this->specials = $escape === $enclosure ? $enclosure : $enclosure . $escape;
    }

    public function endsOutsideAnEnclosure(string $buffer): bool
    {
        $length = strlen($buffer);
        $position = 0;

        while (true) {
            $fieldStart = $position + strspn($buffer, $this->blanks, $position);

            if ($fieldStart < $length && $buffer[$fieldStart] === $this->enclosure) {
                $position = $fieldStart + 1;

                while (true) {
                    $position += strcspn($buffer, $this->specials, $position);

                    if ($position >= $length) {
                        return false;
                    }

                    if ($buffer[$position] !== $this->enclosure) {
                        $position += 2;

                        continue;
                    }

                    if (($position + 1) < $length && $buffer[$position + 1] === $this->enclosure) {
                        $position += 2;

                        continue;
                    }

                    $position++;

                    break;
                }
            }

            $nextSeparator = strpos($buffer, $this->separator, $position);

            if ($nextSeparator === false) {
                return true;
            }

            $position = $nextSeparator + 1;
        }
    }
}

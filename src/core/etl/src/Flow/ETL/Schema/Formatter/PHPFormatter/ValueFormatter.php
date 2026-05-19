<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Formatter\PHPFormatter;

use Flow\ETL\Exception\RuntimeException;

use function array_is_list;
use function get_debug_type;
use function implode;
use function is_array;
use function is_bool;
use function is_numeric;
use function is_string;
use function sprintf;

final class ValueFormatter
{
    public function format(mixed $value): string
    {
        if (null === $value) {
            return 'null';
        }

        if (is_array($value)) {
            return $this->formatArray($value);
        }

        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (is_string($value)) {
            return sprintf('"%s"', $value);
        }

        if (!is_numeric($value)) {
            throw new RuntimeException(sprintf('Unsupported value type: %s', get_debug_type($value)));
        }

        return (string) $value;
    }

    /**
     * @param array<array-key, mixed> $array
     */
    private function formatArray(array $array): string
    {
        $formattedArray = [];

        if (array_is_list($array)) {
            foreach ($array as $value) {
                $formattedArray[] = sprintf('%s', $this->format($value));
            }
        } else {
            foreach ($array as $key => $value) {
                $formattedArray[] = sprintf('%s => %s', $this->format($key), $this->format($value));
            }
        }

        return '[' . implode(', ', $formattedArray) . ']';
    }
}

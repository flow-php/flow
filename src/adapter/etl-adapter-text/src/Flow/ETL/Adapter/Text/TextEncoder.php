<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;
use Stringable;

use function array_values;
use function count;
use function get_debug_type;
use function is_bool;
use function is_scalar;
use function rtrim;
use function sprintf;

/**
 * @implements Encoder<string>
 */
final class TextEncoder implements Encoder
{
    public function __construct(
        private readonly string $newLineSeparator = PHP_EOL,
    ) {}

    public function decode(array $batch): array
    {
        $maps = [];

        foreach ($batch as $line) {
            $maps[] = new RawRowValues(['text' => rtrim($line)]);
        }

        return $maps;
    }

    public function encode(array $batch): array
    {
        $lines = [];

        foreach ($batch as $rowValues) {
            $values = $rowValues->values;

            if (count($values) > 1) {
                throw new RuntimeException(sprintf(
                    'Text data loader supports only a single entry rows, and you have %d rows.',
                    count($values),
                ));
            }

            /** @var mixed $value */
            $value = array_values($values)[0] ?? null;

            $lines[] = $this->renderValue($value) . $this->newLineSeparator;
        }

        return $lines;
    }

    private function renderValue(mixed $value): string
    {
        return match (true) {
            $value === null => '',
            is_bool($value) => $value ? 'true' : 'false',
            is_scalar($value), $value instanceof Stringable => (string) $value,
            default => throw new RuntimeException(
                'Text data loader supports only scalar values, got ' . get_debug_type($value),
            ),
        };
    }
}

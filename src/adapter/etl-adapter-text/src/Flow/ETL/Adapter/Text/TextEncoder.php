<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
use Stringable;

use function count;
use function get_debug_type;
use function is_bool;
use function is_scalar;
use function sprintf;

final class TextEncoder
{
    public function __construct(
        private readonly string $newLineSeparator = PHP_EOL,
    ) {}

    /**
     * @return list<string>
     */
    public function encode(Rows $rows): array
    {
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $columns[] = $rows->column($definition->entry()->name())->values();
        }

        $lines = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            if (count($columns) > 1) {
                throw new RuntimeException(sprintf(
                    'Text data loader supports only a single entry rows, and you have %d rows.',
                    count($columns),
                ));
            }

            $lines[] = $this->renderValue($columns[0][$i] ?? null) . $this->newLineSeparator;
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

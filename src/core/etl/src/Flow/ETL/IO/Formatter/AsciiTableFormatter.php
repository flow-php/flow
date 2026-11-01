<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Formatter;

use Flow\ETL\IO\Formatter;
use Flow\ETL\IO\Formatter\ASCII\ASCIIValue;
use Flow\ETL\Values\Rows;

use function Flow\Types\DSL\type_string;
use function implode;
use function max;
use function str_repeat;

final class AsciiTableFormatter implements Formatter
{
    /**
     * Every cell is measured once, so the column widths cost one pass over the batch.
     */
    public function format(Rows $rows, int|bool $truncate = 20): string
    {
        if ($rows->count() === 0) {
            return '';
        }

        $headers = [];
        $columns = [];
        $widths = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $header = new ASCIIValue(type_string(), $definition->name());
            $cells = [];
            $width = $header->length($truncate);

            // @mago-ignore analysis:mixed-assignment
            foreach ($rows->column($definition->name())->values() as $cell) {
                $value = new ASCIIValue($definition->type(), $cell);
                $width = max($width, $value->length($truncate));
                $cells[] = $value;
            }

            $headers[] = $header;
            $columns[] = $cells;
            $widths[] = $width;
        }

        $line = '+';

        foreach ($widths as $width) {
            $line .= '-' . str_repeat('-', $width) . '-+';
        }

        $table = [$headers];

        for ($index = 0, $count = $rows->count(); $index < $count; $index++) {
            $cells = [];

            foreach ($columns as $column) {
                $cells[] = $column[$index];
            }

            $table[] = $cells;
        }

        $lines = [$line];

        foreach ($table as $position => $cells) {
            $printed = '|';

            foreach ($cells as $column => $cell) {
                $printed .=
                    ' ' . ASCIIValue::mb_str_pad($cell->print($truncate), $widths[$column], ' ', STR_PAD_LEFT) . ' |';
            }

            $lines[] = $printed;

            // the header row is framed above and below
            if ($position === 0) {
                $lines[] = $line;
            }
        }

        $lines[] = $line;

        return implode(PHP_EOL, $lines) . PHP_EOL . "{$rows->count()} rows" . PHP_EOL;
    }
}

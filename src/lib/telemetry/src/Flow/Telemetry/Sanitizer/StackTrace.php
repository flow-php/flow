<?php

declare(strict_types=1);

namespace Flow\Telemetry\Sanitizer;

use Throwable;

use function count;

final readonly class StackTrace
{
    public static function format(Throwable $throwable): string
    {
        /** @var list<array{function: string, line?: int, file?: string, class?: class-string, type?: '->'|'::', args?: array<mixed>}> $trace */
        $trace = $throwable->getTrace();
        $lines = [];

        foreach ($trace as $index => $frame) {
            $location = array_key_exists('file', $frame)
                ? $frame['file'] . '(' . ($frame['line'] ?? 0) . ')'
                : '[internal function]';

            $lines[] =
                '#'
                . $index
                . ' '
                . $location
                . ': '
                . ($frame['class'] ?? '')
                . ($frame['type'] ?? '')
                . $frame['function']
                . '()';
        }

        $lines[] = '#' . count($trace) . ' {main}';

        return implode("\n", $lines);
    }
}

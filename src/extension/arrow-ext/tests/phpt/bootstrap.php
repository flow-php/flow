<?php

declare(strict_types=1);

require __DIR__ . '/../../../../../vendor/autoload.php';

/**
 * `serialize()` of the result, or `class: message <- previous class: message` of what it threw.
 */
function arrow_outcome(callable $fn): string
{
    try {
        return serialize($fn());
    } catch (Throwable $e) {
        $previous = $e->getPrevious();

        return (
            get_class($e)
            . ': '
            . $e->getMessage()
            . ($previous === null ? '' : ' <- ' . get_class($previous) . ': ' . $previous->getMessage())
        );
    }
}

/**
 * Whether an arrow_outcome() is a refusal: the class and message of what the callable threw.
 */
function arrow_refused(string $outcome): bool
{
    return (bool) preg_match('/^[A-Za-z\\\\]+(Exception|Error|Overflow): /', $outcome);
}

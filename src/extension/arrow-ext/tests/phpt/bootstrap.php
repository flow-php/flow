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

/**
 * JSON of the interfaces arrow registers as reflection sees them: methods, parameters, types, optional, variadic, by-ref.
 */
function arrow_interfaces_reflection(): string
{
    $interfaces = [];

    foreach ([
        'Flow\\Parquet\\ParquetEngine',
        'Flow\\Parquet\\ParquetFileReader',
        'Flow\\Parquet\\ParquetFileWriter',
    ] as $name) {
        $class = new ReflectionClass($name);
        $interfaces[$name] = [
            $class->isInterface(),
            array_map(static fn(ReflectionMethod $method): array => [
                $method->getName(),
                (string) $method->getReturnType(),
                array_map(static fn(ReflectionParameter $parameter): array => [
                    $parameter->getName(),
                    (string) $parameter->getType(),
                    $parameter->isOptional(),
                    $parameter->isVariadic(),
                    $parameter->isPassedByReference(),
                ], $method->getParameters()),
            ], $class->getMethods()),
        ];
    }

    return (string) json_encode($interfaces);
}

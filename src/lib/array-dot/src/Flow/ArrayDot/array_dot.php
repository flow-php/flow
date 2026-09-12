<?php

declare(strict_types=1);

namespace Flow\ArrayDot;

use BackedEnum;
use DateTimeImmutable;
use Flow\ArrayDot\Exception\Exception;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Step\Key;
use Flow\ArrayDot\Step\Multimatch;
use Flow\ArrayDot\Step\Wildcard;
use Flow\Types\Type;
use ReflectionEnum;
use ReflectionEnumBackedCase;
use ReflectionNamedType;

use function array_key_exists;
use function array_key_last;
use function array_keys;
use function array_map;
use function array_slice;
use function array_values;
use function count;
use function enum_exists;
use function explode;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function gettype;
use function is_array;
use function is_subclass_of;
use function preg_match;
use function preg_replace;
use function sprintf;
use function str_contains;
use function str_ends_with;
use function str_replace;
use function str_starts_with;
use function trim;
use function var_export;

/**
 * @deprecated Use {@see Path::fromString()} - it returns typed steps with every escape resolved.
 *
 * @throws InvalidPathException
 *
 * @return non-empty-list<string>
 */
function array_dot_steps(string $path): array
{
    if ('' === $path) {
        throw new InvalidPathException("Path can't be empty.");
    }

    // escaped braces are masked like the escaped dot, so they never count as a multimatch
    $path = str_replace(
        ['\\.', '\\{', '\\}'],
        ['__ESCAPED_DOT__', '__ESCAPED_OPENING_BRACE__', '__ESCAPED_CLOSING_BRACE__'],
        $path,
    );

    if (str_contains($path, '{') && !str_ends_with($path, '}')) {
        throw new InvalidPathException('Multimatch must be used at the end of path');
    }

    $multiMatchPath = [];

    if (preg_match('/(\.)({(.*?)})/', $path, $multiMatchPath)) {
        $path = str_replace($multiMatchPath[2], '__MULTIMATCH_PATH__', $path);
    }

    if (str_starts_with($path, '{') && str_contains($path, '}')) {
        $pathSteps = [$path];
    } else {
        $pathSteps = explode('.', $path);
    }

    foreach ($pathSteps as $index => $step) {
        if ($step === '__MULTIMATCH_PATH__') {
            $step = $multiMatchPath[2] ?? throw new InvalidPathException('Multimatch not found');
        }

        // a multimatch keeps its escapes, its paths are parsed again
        $pathSteps[$index] = str_replace(
            ['__ESCAPED_DOT__', '__ESCAPED_OPENING_BRACE__', '__ESCAPED_CLOSING_BRACE__'],
            [str_starts_with($step, '{') ? '\\.' : '.', '\\{', '\\}'],
            $step,
        );
    }

    return $pathSteps;
}

/**
 * @param array<mixed> $array
 *
 * @throws InvalidPathException
 *
 * @return array<mixed>
 */
function array_dot_set(array $array, Path|string $path, mixed $value): array
{
    $path = $path instanceof Path ? $path : Path::fromString($path);

    if ($path->steps[array_key_last($path->steps)] instanceof Multimatch) {
        throw new InvalidPathException(sprintf(
            'Path "%s" ends with a multimatch, array_dot_set() writes to a key or to every element under a wildcard.',
            $path->toString(),
        ));
    }

    $set = static function (array $array, Step $step, Step ...$rest) use (&$set, $value): array {
        if ($step instanceof Key) {
            if ($rest === []) {
                $array[$step->name] = $value;

                return $array;
            }

            $array[$step->name] = $set(
                array_key_exists($step->name, $array) && is_array($array[$step->name]) ? $array[$step->name] : [],
                ...$rest,
            );

            return $array;
        }

        type_instance_of(Wildcard::class)->assert($step);

        foreach (array_keys($array) as $key) {
            $array[$key] = $rest === [] ? $value : $set(type_array()->cast($array[$key]), ...$rest);
        }

        return $array;
    };

    return $set($array, ...$path->steps);
}

/**
 * @deprecated Use {@see array_dot_get()} with `type_integer()` instead.
 *
 * @param array<mixed> $array
 *
 * @throws InvalidPathException
 */
function array_dot_get_int(array $array, Path|string $path): ?int
{
    return array_dot_get($array, $path, type_integer());
}

/**
 * @deprecated Use {@see array_dot_get()} with `type_string()` instead.
 *
 * @param array<mixed> $array
 *
 * @throws InvalidPathException
 */
function array_dot_get_string(array $array, Path|string $path): ?string
{
    return array_dot_get($array, $path, type_string());
}

/**
 * @deprecated Use {@see array_dot_get()} with `type_boolean()` instead.
 *
 * @param array<mixed> $array
 *
 * @throws InvalidPathException
 */
function array_dot_get_bool(array $array, Path|string $path): ?bool
{
    return array_dot_get($array, $path, type_boolean());
}

/**
 * @deprecated Use {@see array_dot_get()} with `type_float()` instead.
 *
 * @param array<mixed> $array
 *
 * @throws InvalidPathException
 */
function array_dot_get_float(array $array, Path|string $path): ?float
{
    return array_dot_get($array, $path, type_float());
}

/**
 * @deprecated Use {@see array_dot_get()} with `type_datetime()` instead.
 *
 * @param array<mixed> $array
 *
 * @throws InvalidPathException
 */
function array_dot_get_datetime(array $array, Path|string $path): ?DateTimeImmutable
{
    $value = array_dot_get($array, $path, type_datetime());

    if ($value === null) {
        return null;
    }

    return $value instanceof DateTimeImmutable ? $value : DateTimeImmutable::createFromInterface($value);
}

/**
 * @deprecated Use {@see array_dot_get()} together with `BackedEnum::tryFrom()` instead.
 *
 * @param array<mixed> $array
 * @param class-string<\BackedEnum> $enumClass
 *
 * @throws Exception
 * @throws InvalidPathException
 */
function array_dot_get_enum(array $array, Path|string $path, string $enumClass): ?BackedEnum
{
    if (!enum_exists($enumClass)) {
        throw new Exception('Enum class does not exist');
    }

    if (!is_subclass_of($enumClass, BackedEnum::class)) {
        throw new Exception('Enum class must be subclass of BackedEnum');
    }

    $reflection = new ReflectionEnum($enumClass);
    $backingType = $reflection->getBackingType();
    $backingTypeName = $backingType instanceof ReflectionNamedType ? $backingType->getName() : '';

    $result = match ($backingTypeName) {
        'int' => array_dot_get($array, $path, type_integer()),
        'string' => array_dot_get($array, $path, type_string()),
        default => throw new Exception('Unsupported enum backing type: ' . $backingTypeName),
    };

    if ($result === null) {
        return null;
    }

    foreach ($reflection->getCases() as $reflectionCase) {
        if (!$reflectionCase instanceof ReflectionEnumBackedCase) {
            continue;
        }

        if ($reflectionCase->getBackingValue() === $result) {
            $enumValue = $reflectionCase->getValue();

            if ($enumValue instanceof BackedEnum) {
                return $enumValue;
            }
        }
    }

    return null;
}

/**
 * @template T
 *
 * @param array<mixed> $array
 * @param Type<T>|null $type optional coercion target — when provided, the resolved leaf value is cast through {@see Type::cast()} (null is always preserved)
 *
 * @throws InvalidPathException
 *
 * @return ($type is null ? mixed : T|null)
 */
function array_dot_get(array $array, Path|string $path, ?Type $type = null): mixed
{
    $path = $path instanceof Path ? $path : Path::fromString($path);
    $steps = $path->steps;

    if ([] === $array && !$steps[0] instanceof Multimatch) {
        $first = $steps[0];

        if (($first instanceof Key || $first instanceof Wildcard) && $first->nullsafe) {
            return null;
        }

        throw new InvalidPathException(sprintf(
            'Path "%s" does not exists in array "%s".',
            $path->toString(),
            preg_replace('/\s+/', '', trim(var_export($array, true))) ?? '',
        ));
    }

    $lastIndex = count($steps) - 1;
    $arraySlice = $array;
    $takenSteps = [];

    foreach ($steps as $index => $step) {
        $takenSteps[] = $step;

        if ($step instanceof Wildcard) {
            $stepsLeft = array_slice($steps, $index + 1);

            if ($stepsLeft === []) {
                return $arraySlice;
            }

            $pathTaken = (new Path($takenSteps))->toString();
            $validatedArrays = array_map(static function (mixed $v) use ($pathTaken): array {
                if (!is_array($v)) {
                    throw new InvalidPathException(
                        "Expected array under path, \"{$pathTaken}\", but got: " . gettype($v),
                    );
                }

                return $v;
            }, $arraySlice);

            $pathLeft = new Path($stepsLeft);
            $results = [];

            foreach ($validatedArrays as $value) {
                try {
                    $results[] = array_dot_get($value, $pathLeft);
                } catch (InvalidPathException $e) {
                    if ($step->nullsafe) {
                        continue;
                    }

                    throw $e;
                }
            }

            return $results;
        }

        if ($step instanceof Multimatch) {
            $results = [];

            foreach ($step->byResultKey() as $resultKey => $subPath) {
                $results[$resultKey] = array_dot_get($arraySlice, $subPath);
            }

            return $results;
        }

        $key = type_instance_of(Key::class)->assert($step);

        if (!array_key_exists($key->name, $arraySlice)) {
            if (!$key->nullsafe) {
                throw new InvalidPathException(sprintf(
                    'Path "%s" does not exists in array "%s".',
                    $path->toString(),
                    preg_replace('/\s+/', '', trim(var_export($array, true))) ?? '',
                ));
            }

            return null;
        }

        if ($index === $lastIndex) {
            return match (true) {
                $type === null => $arraySlice[$key->name],
                $arraySlice[$key->name] === null => null,
                default => $type->cast($arraySlice[$key->name]),
            };
        }

        if (!is_array($arraySlice[$key->name])) {
            throw new InvalidPathException(sprintf(
                'Expected array under path, "%s", but got: %s',
                (new Path($takenSteps))->toString(),
                gettype($arraySlice[$key->name]),
            ));
        }

        $arraySlice = $arraySlice[$key->name];
    }

    return null;
}

/**
 * @param array<mixed> $array
 *
 * @throws InvalidPathException
 *
 * @return array<mixed>
 */
function array_dot_rename(array $array, Path|string $path, string $newName): array
{
    $path = $path instanceof Path ? $path : Path::fromString($path);

    $last = $path->steps[array_key_last($path->steps)];

    if (!$last instanceof Key) {
        throw new InvalidPathException(sprintf(
            'Path "%s" does not end with a key, array_dot_rename() renames a key.',
            $path->toString(),
        ));
    }

    if (!array_dot_exists($array, $path)) {
        throw new InvalidPathException(sprintf(
            'Path "%s" does not exists in array "%s".',
            $path->toString(),
            preg_replace('/\s+/', '', trim(var_export($array, true))) ?? '',
        ));
    }

    if ((string) $last->name === $newName) {
        return $array;
    }

    $rename = static function (array $array, Step $step, Step ...$rest) use (&$rename, $newName): array {
        if ($step instanceof Key) {
            // only a nullsafe key can be absent once the path exists
            if (!array_key_exists($step->name, $array)) {
                return $array;
            }

            if ($rest === []) {
                $array[$newName] = $array[$step->name];
                unset($array[$step->name]);

                return $array;
            }

            $array[$step->name] = $rename(type_array()->assert($array[$step->name]), ...$rest);

            return $array;
        }

        // a wildcard, never the last step - the path ends with a key
        $wildcard = type_instance_of(Wildcard::class)->assert($step);
        $restPath = new Path(array_values($rest));

        foreach (array_keys($array) as $key) {
            $element = type_array()->assert($array[$key]);

            if ($wildcard->nullsafe && !array_dot_exists($element, $restPath)) {
                continue;
            }

            $array[$key] = $rename($element, ...$restPath->steps);
        }

        return $array;
    };

    return $rename($array, ...$path->steps);
}

/**
 * @param array<mixed> $array
 */
function array_dot_exists(array $array, Path|string $path): bool
{
    try {
        array_dot_get($array, $path);

        return true;
    } catch (InvalidPathException) {
        return false;
    }
}

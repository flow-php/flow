<?php

declare(strict_types=1);

namespace Flow\ArrayDot;

use BackedEnum;
use DateTimeImmutable;
use Flow\ArrayDot\Exception\Exception;
use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\Types\Type;
use ReflectionEnum;
use ReflectionEnumBackedCase;
use ReflectionNamedType;

use function array_key_exists;
use function array_map;
use function array_merge;
use function array_pop;
use function array_slice;
use function count;
use function enum_exists;
use function explode;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function gettype;
use function implode;
use function in_array;
use function is_array;
use function is_subclass_of;
use function ltrim;
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
 * @throws InvalidPathException
 *
 * @return non-empty-list<string>
 */
function array_dot_steps(string $path): array
{
    if ('' === $path) {
        throw new InvalidPathException("Path can't be empty.");
    }

    if (str_contains($path, '{') && !str_ends_with($path, '}')) {
        throw new InvalidPathException('Multimatch must be used at the end of path');
    }

    $path = str_replace('\\.', '__ESCAPED_DOT__', $path);

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
        $pathSteps[$index] = str_replace('__ESCAPED_DOT__', '.', $step);

        if ($step === '__MULTIMATCH_PATH__') {
            $pathSteps[$index] = $multiMatchPath[2] ?? throw new InvalidPathException('Multimatch not found');
        }
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
function array_dot_set(array $array, string $path, mixed $value): array
{
    $pathSteps = array_dot_steps($path);
    $lastIndex = count($pathSteps) - 1;

    $newArray = [];
    $currentElement = &$newArray;

    $takenSteps = [];

    foreach ($pathSteps as $index => $step) {
        $takenSteps[] = $step;

        if ($step === '*') {
            $nestedValues = array_dot_get($array, implode('.', $takenSteps), type_array()) ?? [];
            $stepsLeft = array_slice($pathSteps, count($takenSteps), count($pathSteps));

            $nestedArrays = array_map(static fn(mixed $v): array => type_array()->cast($v), $nestedValues);

            foreach ($nestedArrays as $nestedKey => $nestedArray) {
                $currentElement[$nestedKey] = array_dot_set($nestedArray, implode('.', $stepsLeft), $value);
            }

            return $newArray;
        }

        if ($step == '\\*') {
            $step = str_replace('\\', '', $step);
            array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if ($index === $lastIndex) {
            $currentElement[$step] = $value;

            break;
        }

        $currentElement[$step] = [];

        $currentElement = &$currentElement[$step];
    }

    return array_merge($array, $newArray);
}

/**
 * @deprecated Use {@see array_dot_get()} with `type_integer()` instead.
 *
 * @param array<mixed> $array
 *
 * @throws InvalidPathException
 */
function array_dot_get_int(array $array, string $path): ?int
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
function array_dot_get_string(array $array, string $path): ?string
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
function array_dot_get_bool(array $array, string $path): ?bool
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
function array_dot_get_float(array $array, string $path): ?float
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
function array_dot_get_datetime(array $array, string $path): ?DateTimeImmutable
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
function array_dot_get_enum(array $array, string $path, string $enumClass): ?BackedEnum
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
function array_dot_get(array $array, string $path, ?Type $type = null): mixed
{
    if ([] === $array) {
        if (str_starts_with($path, '?')) {
            return null;
        }

        throw new InvalidPathException(sprintf(
            'Path "%s" does not exists in array "%s".',
            $path,
            preg_replace('/\s+/', '', trim(var_export($array, true))) ?? '',
        ));
    }

    $pathSteps = array_dot_steps($path);
    $lastIndex = count($pathSteps) - 1;

    $arraySlice = $array;
    $takenSteps = [];

    foreach ($pathSteps as $index => $step) {
        $takenSteps[] = $step;

        if (in_array($step, ['*', '?*'], true)) {
            $stepsLeft = array_slice($pathSteps, count($takenSteps), count($pathSteps));

            if (!count($stepsLeft)) {
                return $arraySlice;
            }

            $sliceArray = type_array()->assert($arraySlice);
            $pathTaken = implode('.', $takenSteps);
            $stepsLeftJoined = implode('.', $stepsLeft);
            $validatedArrays = array_map(static function (mixed $v) use ($pathTaken): array {
                if (!is_array($v)) {
                    throw new InvalidPathException(
                        "Expected array under path, \"{$pathTaken}\", but got: " . gettype($v),
                    );
                }

                return $v;
            }, $sliceArray);

            $results = [];

            foreach ($validatedArrays as $value) {
                try {
                    $results[] = array_dot_get($value, $stepsLeftJoined);
                } catch (InvalidPathException $e) {
                    if ($step === '?*') {
                        continue;
                    }

                    throw $e;
                }
            }

            return $results;
        }

        // Multiselect
        $subStepsMatch = [];

        if (preg_match('/^{(.*?)}$/', $step, $subStepsMatch)) {
            $sliceArray = type_array()->assert($arraySlice);
            $subSteps = explode(',', $subStepsMatch[1]);
            $results = [];

            foreach ($subSteps as $subStep) {
                $results[str_replace('.', '_', str_replace('?', '', trim($subStep)))] = array_dot_get(
                    $sliceArray,
                    trim($subStep),
                );
            }

            return $results;
        }

        if (in_array($step, ['\\*', '\\?*'], true)) {
            $step = ltrim($step, '\\');
            array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        $nullSafe = false;

        if (str_starts_with($step, '?') && $step !== '?*') {
            $nullSafe = true;
            $step = ltrim($step, '?');
            array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if (str_contains($step, '\\{')) {
            $step = str_replace('\\{', '{', $step);
            array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if (str_contains($step, '\\}')) {
            $step = str_replace('\\}', '}', $step);
            array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        $sliceArray = type_array()->assert($arraySlice);

        if (!array_key_exists($step, $sliceArray)) {
            if (!$nullSafe) {
                throw new InvalidPathException(sprintf(
                    'Path "%s" does not exists in array "%s".',
                    $path,
                    preg_replace('/\s+/', '', trim(var_export($array, true))) ?? '',
                ));
            }

            return null;
        }

        if ($index === $lastIndex) {
            return match (true) {
                $type === null => $sliceArray[$step],
                $sliceArray[$step] === null => null,
                default => $type->cast($sliceArray[$step]),
            };
        }

        if (!is_array($sliceArray[$step])) {
            throw new InvalidPathException(sprintf(
                'Expected array under path, "%s", but got: %s',
                implode('.', $takenSteps),
                gettype($sliceArray[$step]),
            ));
        }

        $arraySlice = type_array()->assert($sliceArray[$step]);
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
function array_dot_rename(array $array, string $path, string $newName): array
{
    if (!array_dot_exists($array, $path)) {
        throw new InvalidPathException(sprintf(
            'Path "%s" does not exists in array "%s".',
            $path,
            preg_replace('/\s+/', '', trim(var_export($array, true))) ?? '',
        ));
    }

    $pathSteps = array_dot_steps($path);
    $lastStep = array_pop($pathSteps);

    $currentElement = &$array;

    $takenSteps = [];

    foreach ($pathSteps as $step) {
        $takenSteps[] = $step;

        if ($step === '*') {
            $nestedValues = array_dot_get($array, implode('.', $takenSteps), type_array()) ?? [];
            $stepsLeft = array_slice($pathSteps, count($takenSteps), count($pathSteps));
            $stepsLeft[] = $lastStep;

            $nestedArrays = array_map(static fn(mixed $v): array => type_array()->cast($v), $nestedValues);

            foreach ($nestedArrays as $nestedKey => $nestedArray) {
                $currentElement[$nestedKey] = array_dot_rename($nestedArray, implode('.', $stepsLeft), $newName);
            }

            return $array;
        }

        if ($step == '\\*') {
            $step = str_replace('\\', '', $step);
            array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if (!is_array($currentElement[$step])) {
            throw new Exception(sprintf(
                'Item for path "%s" is not an array in "%s".',
                implode('.', $takenSteps),
                preg_replace('/\s+/', '', trim(var_export($array, true))) ?? '',
            ));
        }

        $currentElement = &$currentElement[$step];
    }

    $currentElement[$newName] = $currentElement[$lastStep];
    unset($currentElement[$lastStep]);

    return $array;
}

/**
 * @param array<mixed> $array
 */
function array_dot_exists(array $array, string $path): bool
{
    try {
        array_dot_get($array, $path);

        return true;
    } catch (InvalidPathException) {
        return false;
    }
}

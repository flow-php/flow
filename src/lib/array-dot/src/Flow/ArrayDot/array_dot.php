<?php

declare(strict_types=1);

namespace Flow\ArrayDot;

use Flow\ArrayDot\Exception\Exception;
use Flow\ArrayDot\Exception\InvalidPathException;

/**
 * @param string $path
 *
 * @throws InvalidPathException
 *
 * @return array<string>
 */
function array_dot_steps(string $path) : array
{
    if (!\strlen($path)) {
        throw new InvalidPathException("Path can't be empty.");
    }

    if (\str_contains($path, '{')) {
        if (!\str_contains($path, '}')) {
            throw new InvalidPathException('Multimatch syntax not closed');
        }

        if (\strpos($path, '}') !== \strlen($path) - 1) {
            throw new InvalidPathException('Multimatch must be used at the end of path');
        }
    }

    $path = \str_replace('\\.', '__ESCAPED_DOT__', $path);

    if (\preg_match('/(\.)({(.*?)})/', $path, $multiMatchPath)) {
        $path = \str_replace($multiMatchPath[2], '__MULTIMATCH_PATH__', $path);
    }

    if (\str_starts_with($path, '{') && \str_contains($path, '}')) {
        $pathSteps = [$path];
    } else {
        $pathSteps = \explode('.', $path);
    }

    foreach ($pathSteps as $index => $step) {
        $pathSteps[$index] = \str_replace('__ESCAPED_DOT__', '.', $step);

        if ($step === '__MULTIMATCH_PATH__') {
            $pathSteps[$index] = $multiMatchPath[2];
        }
    }

    return $pathSteps;
}

/**
 * @param array<mixed> $array
 * @param string $path
 * @param mixed $value
 *
 * @throws InvalidPathException
 *
 * @return array<mixed>
 */
function array_dot_set(array $array, string $path, $value) : array
{
    $pathSteps = array_dot_steps($path);

    $newArray = [];
    $currentElement = &$newArray;

    $takenSteps = [];

    foreach ($pathSteps as $step) {
        $takenSteps[] = $step;

        if ($step === '*') {
            /**
             * @var array<mixed> $nestedValues
             */
            $nestedValues = array_dot_get($array, \implode('.', $takenSteps));
            $stepsLeft = \array_slice($pathSteps, \count($takenSteps), \count($pathSteps));

            /** @var mixed $nestedValue */
            foreach ($nestedValues as $nestedKey => $nestedValue) {
                $currentElement[$nestedKey] = array_dot_set((array) $nestedValue, \implode('.', $stepsLeft), $value);
            }

            return $newArray;
        }

        if ($step == '\\*') {
            $step = \str_replace('\\', '', $step);
            \array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        $currentElement[$step] = [];

        $currentElement = &$currentElement[$step];
    }

    /**
     * @psalm-suppress MixedAssignment
     */
    $currentElement = $value;

    /** @var array<array-key, mixed> $newArray */
    return \array_merge($array, $newArray);
}

/**
 * @param array<mixed> $array
 * @param string $path
 *
 * @throws InvalidPathException
 *
 * @return mixed
 */
function array_dot_get(array $array, string $path) : mixed
{
    if (\count($array) === 0) {
        if (\str_starts_with($path, '?')) {
            return null;
        }

        throw new InvalidPathException(
            \sprintf(
                'Path "%s" does not exists in array "%s".',
                $path,
                \preg_replace('/\s+/', '', \trim(\var_export($array, true)))
            )
        );
    }

    $pathSteps = array_dot_steps($path);

    $arraySlice = $array;
    /** @var array<string> $takenSteps */
    $takenSteps = [];

    foreach ($pathSteps as $step) {
        $takenSteps[] = $step;

        if (\in_array($step, ['*', '?*'], true)) {
            $stepsLeft = \array_slice($pathSteps, \count($takenSteps), \count($pathSteps));
            $results = [];

            foreach (\array_keys($arraySlice) as $key) {
                if (!\count($stepsLeft)) {
                    return $arraySlice;
                }

                if ($step === '?*') {
                    if (!\is_array($arraySlice[$key])) {
                        $pathTaken = \implode('.', $takenSteps);
                        $type = \gettype($arraySlice[$key]);

                        throw new InvalidPathException("Expected array under path, \"{$pathTaken}\", but got: {$type}");
                    }

                    if (array_dot_exists($arraySlice[$key], \implode('.', $stepsLeft))) {
                        /**
                         * @psalm-suppress MixedAssignment
                         */
                        $results[] = array_dot_get($arraySlice[$key], \implode('.', $stepsLeft));
                    }
                } else {
                    if (!\is_array($arraySlice[$key])) {
                        $pathTaken = \implode('.', $takenSteps);
                        $type = \gettype($arraySlice[$key]);

                        throw new InvalidPathException("Expected array under path, \"{$pathTaken}\", but got: {$type}");
                    }

                    /**
                     * @psalm-suppress MixedAssignment
                     */
                    $results[] = array_dot_get($arraySlice[$key], \implode('.', $stepsLeft));
                }
            }

            return $results;
        }

        // Multiselect
        if (\preg_match('/^{(.*?)}$/', $step, $subSteps)) {
            $subSteps = \explode(',', $subSteps[1]);
            $results = [];

            foreach ($subSteps as $subStep) {
                $subSteps = array_dot_steps(\trim($subStep));

                /** @psalm-suppress MixedAssignment */
                $results[\str_replace('.', '_', \str_replace('?', '', \trim($subStep)))] = array_dot_get($arraySlice, \trim($subStep));
            }

            return $results;
        }

        if (\in_array($step, ['\\*', '\\?*'], true)) {
            $step = \ltrim($step, '\\');
            \array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        $nullSafe = false;

        if (\str_starts_with($step, '?') && $step !== '?*') {
            $nullSafe = true;
            $step = \ltrim($step, '?');
            \array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if (\str_contains($step, '\\{')) {
            $step = \str_replace('\\{', '{', $step);
            \array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if (\str_contains($step, '\\}')) {
            $step = \str_replace('\\}', '}', $step);
            \array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if (!\array_key_exists($step, $arraySlice)) {
            if (!$nullSafe) {
                throw new InvalidPathException(
                    \sprintf(
                        'Path "%s" does not exists in array "%s".',
                        $path,
                        \preg_replace('/\s+/', '', \trim(\var_export($array, true)))
                    )
                );
            }

            return null;
        }

        /** @var array<mixed> $arraySlice */
        $arraySlice = $arraySlice[$step];
    }

    return $arraySlice;
}

/**
 * @param array<mixed> $array
 * @param string $path
 * @param string $newName
 *
 * @throws InvalidPathException
 *
 * @return array<mixed>
 */
function array_dot_rename(array $array, string $path, string $newName) : array
{
    if (!array_dot_exists($array, $path)) {
        throw new InvalidPathException(
            \sprintf(
                'Path "%s" does not exists in array "%s".',
                $path,
                \preg_replace('/\s+/', '', \trim(\var_export($array, true)))
            )
        );
    }

    $pathSteps = array_dot_steps($path);
    $lastStep = \array_pop($pathSteps);

    $currentElement = &$array;

    $takenSteps = [];

    foreach ($pathSteps as $step) {
        $takenSteps[] = $step;

        if ($step === '*') {
            /**
             * @var array<mixed> $nestedValues
             */
            $nestedValues = array_dot_get($array, \implode('.', $takenSteps));
            $stepsLeft = \array_slice($pathSteps, \count($takenSteps), \count($pathSteps));
            \array_push($stepsLeft, $lastStep);

            /** @var mixed $nestedValue */
            foreach ($nestedValues as $nestedKey => $nestedValue) {
                $currentElement[$nestedKey] = array_dot_rename((array) $nestedValue, \implode('.', $stepsLeft), $newName);
            }

            return $array;
        }

        if ($step == '\\*') {
            $step = \str_replace('\\', '', $step);
            \array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if (!\is_array($currentElement[$step])) {
            throw new Exception(
                \sprintf(
                    'Item for path "%s" is not an array in "%s".',
                    \implode('.', $takenSteps),
                    \preg_replace('/\s+/', '', \trim(\var_export($array, true)))
                )
            );
        }

        $currentElement = &$currentElement[$step];
    }

    /**
     * @psalm-suppress MixedAssignment
     */
    $currentElement[$newName] = $currentElement[$lastStep];
    unset($currentElement[$lastStep]);

    return $array;
}

/**
 * @param array<mixed> $array
 * @param string $path
 *
 * @return bool
 */
function array_dot_exists(array $array, string $path) : bool
{
    try {
        array_dot_get($array, $path);

        return true;
    } catch (InvalidPathException) {
        return false;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ArrayDot;

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

    if (\strpos($path, '{') !== false) {
        if (\strpos($path, '}') === false) {
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

    if (\strpos($path, '{') === 0 && \strpos($path, '}') !== false) {
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
function array_dot_get(array $array, string $path)
{
    if (\count($array) === 0) {
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
                    /**
                     * @psalm-suppress MixedArgument
                     */
                    if (array_dot_exists($arraySlice[$key], \implode('.', $stepsLeft))) {
                        /**
                         * @psalm-suppress MixedAssignment
                         * @psalm-suppress MixedArgument
                         */
                        $results[] = array_dot_get($arraySlice[$key], \implode('.', $stepsLeft));
                    }
                } else {
                    /**
                     * @psalm-suppress MixedAssignment
                     * @psalm-suppress MixedArgument
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
                /** @psalm-suppress MixedAssignment */
                $results[] = array_dot_get($arraySlice, \trim($subStep));
            }

            return $results;
        }

        if (\in_array($step, ['\\*', '\\?*'], true)) {
            $step = \ltrim($step, '\\');
            \array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        $nullSafe = false;

        if (\strpos($step, '?') === 0 && $step !== '?*') {
            $nullSafe = true;
            $step = \ltrim($step, '?');
            \array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if (\strpos($step, '\\{') !== false) {
            $step = \str_replace('\\{', '{', $step);
            \array_pop($takenSteps);
            $takenSteps[] = $step;
        }

        if (\strpos($step, '\\}') !== false) {
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
 *
 * @return bool
 */
function array_dot_exists(array $array, string $path) : bool
{
    try {
        array_dot_get($array, $path);

        return true;
    } catch (InvalidPathException $e) {
        return false;
    }
}

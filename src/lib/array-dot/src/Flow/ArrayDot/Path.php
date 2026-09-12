<?php

declare(strict_types=1);

namespace Flow\ArrayDot;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Step\Key;
use Flow\ArrayDot\Step\Multimatch;
use Flow\ArrayDot\Step\Wildcard;

use function array_filter;
use function array_key_last;
use function array_map;
use function array_values;
use function implode;
use function str_contains;
use function strlen;
use function substr;
use function trim;

final readonly class Path
{
    public const string ESCAPABLE = '\\.?*,{}';

    /**
     * @var non-empty-list<Step>
     */
    public array $steps;

    /**
     * @param array<array-key, Step> $steps
     *
     * @throws InvalidPathException
     */
    public function __construct(array $steps)
    {
        if ($steps === []) {
            throw new InvalidPathException("Path can't be empty.");
        }

        foreach ($steps as $index => $step) {
            if ($step instanceof Multimatch && $index !== array_key_last($steps)) {
                throw new InvalidPathException('Multimatch must be used at the end of path');
            }
        }

        $this->steps = array_values($steps);
    }

    /**
     * @throws InvalidPathException
     */
    public static function fromString(string $path): self
    {
        if ($path === '') {
            throw new InvalidPathException("Path can't be empty.");
        }

        $length = strlen($path);
        $steps = [];
        $position = -1;

        do {
            $position++;
            $nullsafe = false;

            while ($position < $length && $path[$position] === '?') {
                $nullsafe = true;
                $position++;
            }

            if ($position < $length && $path[$position] === '{') {
                if ($nullsafe) {
                    throw new InvalidPathException(
                        'Nullsafe "?" cannot precede a multimatch, mark its paths instead: {?a,?b}.',
                    );
                }

                $paths = [];
                $part = '';

                for ($position++; $position < $length && $path[$position] !== '}'; $position++) {
                    if ($path[$position] === '\\' && ($position + 1) < $length) {
                        $part .= $path[$position] . $path[$position + 1];
                        $position++;

                        continue;
                    }

                    if ($path[$position] === ',') {
                        $paths[] = self::fromString(trim($part));
                        $part = '';

                        continue;
                    }

                    $part .= $path[$position];
                }

                if ($position !== ($length - 1)) {
                    throw new InvalidPathException('Multimatch must be used at the end of path');
                }

                $paths[] = self::fromString(trim($part));

                return new self([...$steps, new Multimatch($paths)]);
            }

            $start = $position;
            $name = '';

            while ($position < $length && $path[$position] !== '.') {
                if (
                    $path[$position] === '\\'
                    && ($position + 1) < $length
                    && str_contains(self::ESCAPABLE, $path[$position + 1])
                ) {
                    $name .= $path[$position + 1];
                    $position += 2;

                    continue;
                }

                $name .= $path[$position];
                $position++;
            }

            $steps[] = substr($path, $start, $position - $start) === '*'
                ? new Wildcard($nullsafe)
                : new Key($name, $nullsafe);
        } while ($position < $length);

        return new self($steps);
    }

    public function selectsSingleValue(): bool
    {
        return array_filter($this->steps, static fn(Step $step): bool => !$step instanceof Key) === [];
    }

    public function toString(): string
    {
        return implode('.', array_map(static fn(Step $step): string => $step->toString(), $this->steps));
    }
}

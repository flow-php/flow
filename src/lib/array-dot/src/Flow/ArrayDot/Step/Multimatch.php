<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Step;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step;

use function array_map;
use function implode;

final readonly class Multimatch implements Step
{
    /**
     * @var non-empty-list<Path>
     */
    public array $paths;

    /**
     * @param list<Path> $paths
     *
     * @throws InvalidPathException
     */
    public function __construct(array $paths)
    {
        if ($paths === []) {
            throw new InvalidPathException('Multimatch needs at least one path.');
        }

        foreach ($paths as $path) {
            foreach ($path->steps as $step) {
                if ($step instanceof self) {
                    throw new InvalidPathException('Multimatch cannot contain another multimatch.');
                }
            }
        }

        $this->paths = $paths;
    }

    /**
     * @return array<array-key, Path>
     */
    public function byResultKey(): array
    {
        $keyed = [];

        foreach ($this->paths as $path) {
            $keyed[implode('_', array_map(static fn(Step $step): string => $step instanceof Key
                ? (string) $step->name
                : '*', $path->steps))] = $path;
        }

        return $keyed;
    }

    public function toString(): string
    {
        return '{' . implode(',', array_map(static fn(Path $path): string => $path->toString(), $this->paths)) . '}';
    }
}

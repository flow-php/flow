<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Flow\Doctrine\Bulk\Exception\RuntimeException;

final class Columns
{
    /**
     * @var string[]
     */
    private array $columns;

    public function __construct(string ...$columns)
    {
        if (\count($columns) === 0) {
            throw new RuntimeException('Columns cannot be empty');
        }

        if (\array_unique($columns) !== $columns) {
            throw new RuntimeException('All columns must be unique');
        }

        $this->columns = $columns;
    }

    public function suffix(string $suffix) : self
    {
        return new self(
            ...$this->map(
                fn (string $column) : string => $column . $suffix
            )
        );
    }

    public function prefix(string $prefix) : self
    {
        return new self(
            ...$this->map(
                fn (string $column) : string => $prefix . $column
            )
        );
    }

    public function concat(string $separator) : string
    {
        return \implode($separator, $this->columns);
    }

    /**
     * @return string[]
     */
    public function all() : array
    {
        return $this->columns;
    }

    /**
     * @template ReturnType
     * @psalm-param callable(string) : ReturnType $callable
     * @psalm-return ReturnType[]
     */
    public function map(callable $callable) : array
    {
        return \array_map($callable, $this->columns);
    }
}

<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Flow\Doctrine\Bulk\Exception\RuntimeException;

final class Columns
{
    /**
     * @var array<string>
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
     * @return array<string>
     */
    public function all() : array
    {
        return $this->columns;
    }

    /**
     * @template ReturnType
     * @psalm-param callable(string) : ReturnType $callable
     * @psalm-return array<ReturnType>
     */
    public function map(callable $callable) : array
    {
        /** @var array<ReturnType> $columns */
        $columns = [];

        foreach ($this->columns as $column) {
            $columns[] = $callable($column);
        }

        return $columns;
    }

    /**
     * @param string ...$columnNames
     *
     * @return bool
     */
    public function has(string ...$columnNames) : bool
    {
        return \count(\array_unique(\array_merge($this->columns, $columnNames))) === \count($this->columns);
    }

    /**
     * @throws RuntimeException
     */
    public function without(string ...$columnNames) : self
    {
        $columns = [];

        foreach ($this->columns as $column) {
            if (false === \in_array($column, $columnNames, true)) {
                $columns[] = $column;
            }
        }

        return new self(...$columns);
    }
}

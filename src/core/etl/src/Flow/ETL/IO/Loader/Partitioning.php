<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;

use function array_values;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final readonly class Partitioning
{
    private function __construct(
        public References $by,
        public bool $writeColumns = false,
    ) {}

    public static function by(string|Reference $column, string|Reference ...$columns): self
    {
        return new self(References::init($column, ...$columns));
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        $by = type_list(type_string())->assert($data['by'] ?? null);
        $partitioning = $by === [] ? self::none() : self::by(...$by);

        return type_boolean()->assert($data['write_columns'] ?? null) ? $partitioning->writeColumns() : $partitioning;
    }

    public static function none(): self
    {
        return new self(References::init());
    }

    /**
     * @return array{by: list<string>, write_columns: bool}
     */
    public function normalize(): array
    {
        return ['by' => array_values($this->by->names()), 'write_columns' => $this->writeColumns];
    }

    /**
     * Keep the partition columns in the file body as well as in the path. Off by default.
     */
    public function writeColumns(bool $writeColumns = true): self
    {
        if (!$this->by->count()) {
            throw new InvalidArgumentException('writeColumns requires at least one partition column');
        }

        return new self($this->by, $writeColumns);
    }
}

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
use function is_array;

final readonly class Partitioning
{
    private function __construct(
        public References $by,
        public bool $writeColumns = false,
    ) {}

    /**
     * @param non-empty-list<Reference|string>|Reference|string $columns
     * @param bool $writeColumns keep the partition columns in the file body as well as in the path
     *
     * @throws InvalidArgumentException when $columns is an empty list
     */
    public static function by(array|string|Reference $columns, bool $writeColumns = false): self
    {
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($columns === []) {
            throw new InvalidArgumentException('partitionBy() needs at least one column, an empty list was given');
        }

        return new self(References::init(...is_array($columns) ? $columns : [$columns]), $writeColumns);
    }

    /**
     * @param array<array-key, mixed> $data the shape of normalize()
     */
    public static function fromArray(array $data): self
    {
        $by = type_list(type_string())->assert($data['by'] ?? null);

        return $by === [] ? self::none() : self::by($by, type_boolean()->assert($data['write_columns'] ?? null));
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
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\QueryBuilder\Sql;

use function Flow\PostgreSql\DSL\create;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

/**
 * @type SequenceShape = array{name: string, data_type: string, start_value: null|int|string, min_value: null|int|string, max_value: null|int|string, increment_by: int|string, cycle: bool, cache_value: int|string, owned_by_table: ?string, owned_by_column: ?string, schema?: ?string}
 */
final readonly class Sequence
{
    public function __construct(
        public string $name,
        public string $dataType = 'bigint',
        public int|string|null $startValue = null,
        public int|string|null $minValue = null,
        public int|string|null $maxValue = null,
        public int|string $incrementBy = 1,
        public bool $cycle = false,
        public int|string $cacheValue = 1,
        public ?string $ownedByTable = null,
        public ?string $ownedByColumn = null,
        public ?string $schema = null,
    ) {}

    /**
     * @param SequenceShape $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            name: $data['name'],
            dataType: $data['data_type'],
            startValue: $data['start_value'] ?? null,
            minValue: $data['min_value'] ?? null,
            maxValue: $data['max_value'] ?? null,
            incrementBy: $data['increment_by'],
            cycle: $data['cycle'],
            cacheValue: $data['cache_value'],
            ownedByTable: $data['owned_by_table'] ?? null,
            ownedByColumn: $data['owned_by_column'] ?? null,
            schema: $data['schema'] ?? null,
        );
    }

    /**
     * @return SequenceShape
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'data_type' => $this->dataType,
            'start_value' => $this->startValue,
            'min_value' => $this->minValue,
            'max_value' => $this->maxValue,
            'increment_by' => $this->incrementBy,
            'cycle' => $this->cycle,
            'cache_value' => $this->cacheValue,
            'owned_by_table' => $this->ownedByTable,
            'owned_by_column' => $this->ownedByColumn,
            'schema' => $this->schema,
        ];
    }

    public function qualifiedName(): string
    {
        return $this->schemaName() . '.' . $this->name;
    }

    public function schemaName(): string
    {
        return $this->schema ?? 'public';
    }

    public function withSchema(string $schema): self
    {
        return new self(
            $this->name,
            $this->dataType,
            $this->startValue,
            $this->minValue,
            $this->maxValue,
            $this->incrementBy,
            $this->cycle,
            $this->cacheValue,
            $this->ownedByTable,
            $this->ownedByColumn,
            $schema,
        );
    }

    public function effectiveMaxValue(): int|string
    {
        if ($this->maxValue !== null) {
            return $this->maxValue;
        }

        if ((int) $this->incrementBy < 0) {
            return -1;
        }

        return match ($this->dataType) {
            'smallint' => 32_767,
            'integer' => 2_147_483_647,
            default => PHP_INT_MAX,
        };
    }

    public function effectiveMinValue(): int|string
    {
        if ($this->minValue !== null) {
            return $this->minValue;
        }

        if ((int) $this->incrementBy > 0) {
            return 1;
        }

        return match ($this->dataType) {
            'smallint' => -32_768,
            'integer' => -2_147_483_648,
            default => PHP_INT_MIN,
        };
    }

    public function effectiveStartValue(): int|string
    {
        if ($this->startValue !== null) {
            return $this->startValue;
        }

        return (int) $this->incrementBy > 0 ? $this->effectiveMinValue() : $this->effectiveMaxValue();
    }

    public function toSql(): Sql
    {
        $builder = create()->sequence($this->name, $this->schemaName())->asType($this->dataType);

        if ($this->startValue !== null) {
            $builder = $builder->startWith((int) $this->startValue);
        }

        $builder = $builder->incrementBy((int) $this->incrementBy);
        $builder = $this->minValue !== null ? $builder->minValue((int) $this->minValue) : $builder->noMinValue();
        $builder = $builder->cache((int) $this->cacheValue);

        if ($this->maxValue !== null) {
            $builder = $builder->maxValue((int) $this->maxValue);
        } else {
            $builder = $builder->noMaxValue();
        }

        if ($this->cycle) {
            $builder = $builder->cycle();
        }

        if ($this->ownedByTable !== null && $this->ownedByColumn !== null) {
            $builder = $builder->ownedBy($this->schemaName() . '.' . $this->ownedByTable, $this->ownedByColumn);
        }

        return $builder;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Adapter\Parquet\ValueConverter\ValueConverter;
use Flow\ETL\Adapter\Parquet\ValueConverter\ValueConverters;
use Flow\ETL\Rows;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;

use function array_key_exists;

final class ParquetEncoder
{
    /**
     * @var array<string, ValueConverter>
     */
    private array $encodePlan;

    public function __construct(ParquetSchema $schema)
    {
        $encodePlan = [];

        foreach ($schema->columns() as $column) {
            $valueConverter = ValueConverters::for($column);

            if ($valueConverter !== null) {
                $encodePlan[$column->name()] = $valueConverter;
            }
        }

        $this->encodePlan = $encodePlan;
    }

    /**
     * @return list<array<array-key, mixed>>
     */
    public function encode(Rows $rows): array
    {
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $columns[$definition->entry()->name()] = $rows->column($definition->entry()->name())->values();
        }

        $encoded = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $values = [];

            foreach ($columns as $name => $column) {
                $values[$name] = $column[$i];
            }

            foreach ($this->encodePlan as $name => $valueConverter) {
                if (array_key_exists($name, $values) && $values[$name] !== null) {
                    $values[$name] = $valueConverter->encode($values[$name]);
                }
            }

            $encoded[] = $values;
        }

        return $encoded;
    }
}

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
     * @return array<string, list<mixed>> the values of every column as the Parquet writer takes them
     */
    public function columns(Rows $rows): array
    {
        $columns = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $name = $definition->entry()->name();
            $values = $rows->column($name)->values();

            if (array_key_exists($name, $this->encodePlan)) {
                $valueConverter = $this->encodePlan[$name];

                // @mago-ignore analysis:mixed-assignment
                foreach ($values as $i => $value) {
                    if ($value !== null) {
                        $values[$i] = $valueConverter->encode($value);
                    }
                }
            }

            $columns[$name] = $values;
        }

        return $columns;
    }
}

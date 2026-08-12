<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Adapter\Parquet\ValueConverter\ValueConverter;
use Flow\ETL\Adapter\Parquet\ValueConverter\ValueConverters;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Types\Type;

use function array_key_exists;

/**
 * @implements Encoder<array<string, mixed>>
 */
final class ParquetEncoder implements Encoder
{
    /**
     * @var array<string, ValueConverter>
     */
    private array $decodePlan;

    /**
     * @var array<string, array{Type<mixed>, ?ValueConverter}>
     */
    private array $encodePlan;

    public function __construct(ParquetSchema $schema)
    {
        $converter = new SchemaConverter();
        $decodePlan = [];
        $encodePlan = [];

        foreach ($schema->columns() as $column) {
            $valueConverter = ValueConverters::for($column);

            $encodePlan[$column->name()] = [$converter->parquetToFlowType($column), $valueConverter];

            if ($valueConverter !== null) {
                $decodePlan[$column->name()] = $valueConverter;
            }
        }

        $this->decodePlan = $decodePlan;
        $this->encodePlan = $encodePlan;
    }

    public function decode(array $batch): array
    {
        $decoded = [];

        foreach ($batch as $values) {
            foreach ($this->decodePlan as $name => $valueConverter) {
                if (array_key_exists($name, $values) && $values[$name] !== null) {
                    $values[$name] = $valueConverter->decode($values[$name]);
                }
            }

            $decoded[] = new RawRowValues($values);
        }

        return $decoded;
    }

    public function encode(array $batch): array
    {
        $encoded = [];

        foreach ($batch as $rowValues) {
            $values = $rowValues->values;

            foreach ($this->encodePlan as $name => [$type, $valueConverter]) {
                if (array_key_exists($name, $values) && $values[$name] !== null) {
                    $values[$name] = $type->cast($values[$name]);

                    if ($valueConverter !== null) {
                        $values[$name] = $valueConverter->encode($values[$name]);
                    }
                }
            }

            $encoded[] = $values;
        }

        return $encoded;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Types\Type;

use function array_key_exists;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_uuid;
use function is_object;

/**
 * @implements Encoder<array<string, mixed>>
 */
final class ParquetEncoder implements Encoder
{
    /**
     * @var array<string, Type<mixed>>
     */
    private array $decodePlan;

    /**
     * @var array<string, array{Type<mixed>, bool}>
     */
    private array $encodePlan;

    public function __construct(ParquetSchema $schema)
    {
        $converter = new SchemaConverter();
        $decodePlan = [];
        $encodePlan = [];

        foreach ($schema->columns() as $column) {
            $name = $column->name();
            $logicalType = $column instanceof FlatColumn ? $column->logicalType()?->name() : null;

            $encodePlan[$name] = [
                $converter->parquetToFlowType($column),
                $logicalType === LogicalType::UUID || $logicalType === LogicalType::JSON,
            ];

            if ($logicalType === LogicalType::UUID) {
                $decodePlan[$name] = type_uuid();
            } elseif ($logicalType === LogicalType::JSON) {
                $decodePlan[$name] = type_json();
            }
        }

        $this->decodePlan = $decodePlan;
        $this->encodePlan = $encodePlan;
    }

    public function decode(array $batch): array
    {
        $decoded = [];

        foreach ($batch as $values) {
            foreach ($this->decodePlan as $name => $type) {
                if (array_key_exists($name, $values) && $values[$name] !== null) {
                    $values[$name] = $type->cast($values[$name]);
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

            foreach ($this->encodePlan as $name => [$type, $stringify]) {
                if (array_key_exists($name, $values) && $values[$name] !== null) {
                    // @mago-ignore analysis:mixed-assignment
                    $cast = $type->cast($values[$name]);
                    $values[$name] = $stringify && is_object($cast) ? type_string()->cast($cast) : $cast;
                }
            }

            $encoded[] = $values;
        }

        return $encoded;
    }
}

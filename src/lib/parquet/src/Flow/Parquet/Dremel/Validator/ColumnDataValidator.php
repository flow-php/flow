<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\Validator;

use DateInterval;
use DateTimeInterface;
use Flow\Parquet\Dremel\Validator;
use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;

use function gettype;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function sprintf;

final class ColumnDataValidator implements Validator
{
    public function validate(Column $column, mixed $data): void
    {
        $repetition = $column->repetition();

        if ($repetition === Repetition::REQUIRED) {
            if ($data === null) {
                throw new ValidationException(sprintf('Column "%s" is required', $column->flatPath()));
            }
        }

        if ($repetition === Repetition::REPEATED && !is_array($data)) {
            throw new ValidationException(sprintf(
                'Column "%s" is not array, got %s',
                $column->flatPath(),
                gettype($data),
            ));
        }

        if ($repetition === Repetition::OPTIONAL) {
            if ($data === null) {
                return;
            }
        }

        if ($column instanceof FlatColumn) {
            $this->validateData($column, $data, $repetition);

            return;
        }

        /**
         * @var NestedColumn $column
         */
        if ($column->isList()) {
            if (!is_array($data)) {
                throw new ValidationException(sprintf(
                    'Column "%s" is not array, got %s',
                    $column->flatPath(),
                    gettype($data),
                ));
            }

            // @mago-ignore analysis:mixed-assignment
            foreach ($data as $value) {
                $this->validate($column->getListElement(), $value);
            }

            return;
        }

        if ($column->isMap()) {
            if (!is_array($data)) {
                throw new ValidationException(sprintf(
                    'Column "%s" is not array, got %s',
                    $column->flatPath(),
                    gettype($data),
                ));
            }

            $valueColumn = $column->getMapValueColumn();

            // @mago-ignore analysis:mixed-assignment
            foreach ($data as $key => $value) {
                $this->validate($column->getMapKeyColumn(), $key);

                if ($valueColumn !== null) {
                    $this->validate($valueColumn, $value);
                }
            }

            return;
        }

        if (!is_array($data)) {
            throw new ValidationException(sprintf(
                'Column "%s" is not array, got %s',
                $column->flatPath(),
                gettype($data),
            ));
        }

        foreach ($column->children() as $child) {
            $this->validate($child, $data[$child->name()] ?? null);
        }
    }

    private function validateData(FlatColumn $column, mixed $data, ?Repetition $repetition): void
    {
        if (is_array($data)) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($data as $value) {
                $this->validateData($column, $value, $repetition);
            }

            return;
        }

        if ($repetition !== Repetition::REQUIRED) {
            if ($data === null) {
                return;
            }
        }

        $type = $column->type();
        $logicalTypeName = $column->logicalType()?->name();

        switch ($type) {
            case PhysicalType::BOOLEAN:
                if (!is_bool($data)) {
                    throw new ValidationException(sprintf('Column "%s" is not boolean', $column->flatPath()));
                }

                break;
            case PhysicalType::INT64:
            case PhysicalType::INT32:
                switch ($logicalTypeName) {
                    case LogicalType::DATE:
                    case LogicalType::TIMESTAMP:
                        if (!$data instanceof DateTimeInterface) {
                            throw new ValidationException(sprintf(
                                'Column "%s" require \DateTimeInterface as value',
                                $column->flatPath(),
                            ));
                        }

                        break;
                    case LogicalType::TIME:
                        if (!$data instanceof DateInterval) {
                            throw new ValidationException(sprintf(
                                'Column "%s" require \DateInterval as value',
                                $column->flatPath(),
                            ));
                        }

                        break;
                    case null:
                        if (!is_int($data)) {
                            throw new ValidationException(sprintf(
                                'Column "%s" require integer as value, got: %s instead',
                                $column->flatPath(),
                                gettype($data),
                            ));
                        }

                        break;
                }

                break;
            case PhysicalType::FLOAT:
            case PhysicalType::DOUBLE:
                if (!is_float($data)) {
                    throw new ValidationException(sprintf('Column "%s" is not float', $column->flatPath()));
                }

                break;
            case PhysicalType::BYTE_ARRAY:
                switch ($logicalTypeName) {
                    case LogicalType::STRING:
                    case LogicalType::JSON:
                    case LogicalType::UUID:
                        if (!is_string($data)) {
                            throw new ValidationException(sprintf(
                                'Column "%s" is not string, got "%s" instead',
                                $column->flatPath(),
                                gettype($data),
                            ));
                        }

                        break;
                }

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                break;

            default:
                throw new ValidationException(sprintf('Unknown column type "%s"', $type->name));
        }
    }
}

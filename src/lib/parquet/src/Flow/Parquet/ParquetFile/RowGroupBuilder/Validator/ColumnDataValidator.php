<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\Validator;

use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Validator;
use Flow\Parquet\ParquetFile\Schema\{Column, FlatColumn, LogicalType, NestedColumn, PhysicalType, Repetition};

final class ColumnDataValidator implements Validator
{
    public function validate(Column $column, mixed $data) : void
    {
        if ($column->repetition()?->isRequired()) {
            if ($data === null) {
                throw new ValidationException(\sprintf('Column "%s" is required', $column->flatPath()));
            }
        }

        if ($column->repetition()?->isRepeated() && !\is_array($data)) {
            throw new ValidationException(\sprintf('Column "%s" is not array, got %s', $column->flatPath(), \gettype($data)));
        }

        if ($column->repetition() === Repetition::OPTIONAL) {
            if ($data === null) {
                return;
            }
        }

        if ($column instanceof FlatColumn) {
            $this->validateData($column, $data);

            return;
        }

        /**
         * @var NestedColumn $column
         */
        if ($column->isList()) {
            if (!\is_array($data)) {
                throw new ValidationException(\sprintf('Column "%s" is not array, got %s', $column->flatPath(), \gettype($data)));
            }

            foreach ($data as $value) {
                $this->validate($column->getListElement(), $value);
            }

            return;
        }

        if ($column->isMap()) {
            if (!\is_array($data)) {
                throw new ValidationException(\sprintf('Column "%s" is not array, got %s', $column->flatPath(), \gettype($data)));
            }

            foreach ($data as $key => $value) {
                $this->validate($column->getMapKeyColumn(), $key);
                $this->validate($column->getMapValueColumn(), $value);
            }

            return;
        }

        if (!\is_array($data)) {
            throw new ValidationException(\sprintf('Column "%s" is not array, got %s', $column->flatPath(), \gettype($data)));
        }

        foreach ($column->children() as $key => $child) {
            $this->validate($child, $data[$child->name()] ?? null);
        }
    }

    private function validateData(FlatColumn $column, mixed $data) : void
    {
        if (\is_array($data)) {
            foreach ($data as $value) {
                $this->validateData($column, $value);
            }

            return;
        }

        if ($column->repetition()?->isOptional()) {
            if ($data === null) {
                return;
            }
        }

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                if (!\is_bool($data)) {
                    throw new ValidationException(\sprintf('Column "%s" is not boolean', $column->flatPath()));
                }

                break;
            case PhysicalType::INT64:
            case PhysicalType::INT32:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DATE:
                    case LogicalType::TIMESTAMP:
                        if (!$data instanceof \DateTimeInterface) {
                            throw new ValidationException(\sprintf('Column "%s" require \DateTimeInterface as value', $column->flatPath()));
                        }

                        break;
                    case LogicalType::TIME:
                        if (!$data instanceof \DateInterval) {
                            throw new ValidationException(\sprintf('Column "%s" require \DateInterval as value', $column->flatPath()));
                        }

                        break;
                    case null:
                        if (!\is_int($data)) {
                            throw new ValidationException(\sprintf('Column "%s" require integer as value, got: %s instead', $column->flatPath(), \gettype($data)));
                        }

                        break;
                }

                break;
            case PhysicalType::FLOAT:
            case PhysicalType::DOUBLE:
                if (!\is_float($data)) {
                    throw new ValidationException(\sprintf('Column "%s" is not float', $column->flatPath()));
                }

                break;
            case PhysicalType::BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::STRING:
                    case LogicalType::JSON:
                    case LogicalType::UUID:
                        if (!\is_string($data)) {
                            throw new ValidationException(\sprintf('Column "%s" is not string, got "%s" instead', $column->flatPath(), \gettype($data)));
                        }

                        break;
                }

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                break;

            default:
                throw new ValidationException(\sprintf('Unknown column type "%s"', $column->type()->name));
        }
    }
}

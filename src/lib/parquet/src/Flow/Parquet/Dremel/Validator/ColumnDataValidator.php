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
use Flow\Parquet\ParquetFile\Schema\LogicalType\Integer;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Stringable;

use function ctype_xdigit;
use function get_debug_type;
use function gettype;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_object;
use function is_string;
use function mb_check_encoding;
use function method_exists;
use function sprintf;
use function str_replace;
use function strlen;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class ColumnDataValidator implements Validator
{
    public function validate(Column $column, mixed $data, int $row): void
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
            $this->validateData($column, $data, $repetition, $row);

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
                $this->validate($column->getListElement(), $value, $row);
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
                $this->validate($column->getMapKeyColumn(), $key, $row);

                if ($valueColumn !== null) {
                    $this->validate($valueColumn, $value, $row);
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
            $this->validate($child, $data[$child->name()] ?? null, $row);
        }
    }

    private function validateData(FlatColumn $column, mixed $data, ?Repetition $repetition, int $row): void
    {
        if (is_array($data)) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($data as $value) {
                $this->validateData($column, $value, $repetition, $row);
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

        match ($type) {
            PhysicalType::BOOLEAN => is_bool($data)
                ? null
                : throw new ValidationException(sprintf('Column "%s" is not boolean', $column->flatPath())),
            PhysicalType::INT64, PhysicalType::INT32 => match ($logicalTypeName) {
                LogicalType::DATE, LogicalType::TIMESTAMP => $data instanceof DateTimeInterface
                    ? null
                    : throw new ValidationException(sprintf(
                        'Column "%s" require \DateTimeInterface as value',
                        $column->flatPath(),
                    )),
                LogicalType::TIME => $data instanceof DateInterval
                    ? null
                    : throw new ValidationException(sprintf(
                        'Column "%s" require \DateInterval as value',
                        $column->flatPath(),
                    )),
                null => is_int($data)
                    ? null
                    : throw new ValidationException(sprintf(
                        'Column "%s" require integer as value, got: %s instead',
                        $column->flatPath(),
                        gettype($data),
                    )),
                default => null,
            },
            PhysicalType::FLOAT, PhysicalType::DOUBLE => is_float($data)
                ? null
                : throw new ValidationException(sprintf('Column "%s" is not float', $column->flatPath())),
            PhysicalType::BYTE_ARRAY => match ($logicalTypeName) {
                LogicalType::STRING, LogicalType::JSON, LogicalType::ENUM, LogicalType::UUID => is_string($data)
                    ? null
                    : throw new ValidationException(sprintf(
                        'Column "%s" is not string, got "%s" instead',
                        $column->flatPath(),
                        gettype($data),
                    )),
                default => null,
            },
            PhysicalType::FIXED_LEN_BYTE_ARRAY => null,
            default => throw new ValidationException(sprintf('Unknown column type "%s"', $type->name)),
        };

        if (is_int($data) && ($type === PhysicalType::INT32 || $type === PhysicalType::INT64)) {
            $integer = Integer::forColumn($column);
            [$name, $min, $max] = match (true) {
                $integer === null && $type === PhysicalType::INT32 && $column->logicalType() === null => [
                    'INT32',
                    -2_147_483_648,
                    2_147_483_647,
                ],
                $integer === null => ['INT64', PHP_INT_MIN, PHP_INT_MAX],
                $integer->isSigned() => [
                    'INT_' . $integer->bitWidth(),
                    $integer->bitWidth() === 64 ? PHP_INT_MIN : -(1 << ($integer->bitWidth() - 1)),
                    $integer->bitWidth() === 64 ? PHP_INT_MAX : (1 << ($integer->bitWidth() - 1)) - 1,
                ],
                default => [
                    'UINT_' . $integer->bitWidth(),
                    0,
                    $integer->bitWidth() === 64 ? PHP_INT_MAX : (1 << $integer->bitWidth()) - 1,
                ],
            };

            if ($data < $min || $data > $max) {
                throw new ValidationException(sprintf(
                    'Column "%s" row %d: %d is out of the %s range [%d, %d]',
                    $column->flatPath(),
                    $row,
                    $data,
                    $name,
                    $min,
                    $max,
                ));
            }
        }

        if ($logicalTypeName === LogicalType::UUID) {
            $hex = match (true) {
                is_string($data) => str_replace('-', '', $data),
                is_object($data) && method_exists($data, 'toString') => is_string($data->toString())
                    ? str_replace('-', '', (string) $data->toString())
                    : '',
                $data instanceof Stringable => str_replace('-', '', (string) $data),
                default => '',
            };

            if (strlen($hex) !== 32 || !ctype_xdigit($hex)) {
                throw new ValidationException(sprintf(
                    'Column "%s" row %d: %s is not a UUID of 32 hexadecimal digits',
                    $column->flatPath(),
                    $row,
                    get_debug_type($data),
                ));
            }
        }

        if (
            $type === PhysicalType::FIXED_LEN_BYTE_ARRAY
            && $logicalTypeName !== LogicalType::DECIMAL
            && $logicalTypeName !== LogicalType::UUID
            && is_string($data)
            && strlen($data) !== $column->typeLength()
        ) {
            throw new ValidationException(sprintf(
                'Column "%s" row %d: a FIXED_LEN_BYTE_ARRAY(%d) value is %d bytes long',
                $column->flatPath(),
                $row,
                (int) $column->typeLength(),
                strlen($data),
            ));
        }

        if (
            (
                $logicalTypeName === LogicalType::STRING
                || $logicalTypeName === LogicalType::JSON
                || $logicalTypeName === LogicalType::ENUM
            )
            && is_string($data)
            && !mb_check_encoding($data, 'UTF-8')
        ) {
            throw new ValidationException(sprintf(
                'Column "%s" row %d: the string is not valid UTF-8',
                $column->flatPath(),
                $row,
            ));
        }
    }
}

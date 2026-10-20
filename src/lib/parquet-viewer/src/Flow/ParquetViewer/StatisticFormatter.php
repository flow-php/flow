<?php

declare(strict_types=1);

namespace Flow\ParquetViewer;

use Coduo\PHPHumanizer\StringHumanizer;
use DateInterval;
use DateTimeInterface;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\ColumnPrimitiveType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_scalar;
use function Flow\Types\DSL\type_union;

final readonly class StatisticFormatter
{
    public function __construct(
        private DataConverter $converter,
    ) {}

    public function format(FlatColumn $column, bool|float|int|string|null $physical): string
    {
        if ($physical === null) {
            return '-';
        }

        $value = type_union(
            type_scalar(),
            type_instance_of(DateTimeInterface::class),
            type_instance_of(DateInterval::class),
        )->assert($this->converter->fromParquetType($column, $physical));

        return match (true) {
            $value instanceof DateTimeInterface => $value->format('Y-m-d H:i:s.u'),
            $value instanceof DateInterval => $value->format('%H:%I:%S.%F'),
            ColumnPrimitiveType::isString($column) && $column->logicalType()?->is(LogicalType::UUID) !== true
                => StringHumanizer::truncate((string) $value, 20, '...'),
            default => (string) $value,
        };
    }
}

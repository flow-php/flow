<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Bucketing;

use Flow\ETL\Kernels\Comparison\ValueEquality;
use Flow\ETL\Values\Column\ComparableValues;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;

use function array_fill;
use function array_key_exists;
use function count;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function serialize;
use function strlen;

/**
 * Two rows get one key exactly when their values are equal under ComparableValues::equality(): -0.0 is 0, NaN is NaN,
 * and every value is type-tagged, so the string "1" never equals "1.0" or the int 1. A column cast to a type first
 * (a join key) is tagged in that type. A column the batch does not carry is a null where $declared declares it
 * nullable; anywhere else the batch must carry it.
 */
final readonly class KeyEncoder
{
    /**
     * @param list<string> $columns
     * @param array<int, Type<mixed>> $types by position: the type a column is cast to before it is tagged
     * @param null|Schema $declared the bound schema of the batches, when the caller has one
     */
    public function __construct(
        private array $columns,
        private array $types = [],
        private ?Schema $declared = null,
    ) {}

    /**
     * @return list<string> one key per row
     */
    public function of(Rows $rows): array
    {
        if ($rows->isEmpty()) {
            return [];
        }

        $count = count($rows);
        $keys = array_fill(0, $count, '');
        $comparable = new ComparableValues();
        $equality = new ValueEquality();

        foreach ($this->columns as $position => $name) {
            if (
                $rows->schema()->findDefinition($name) === null
                && $this->declared?->findDefinition($name)?->isNullable() === true
            ) {
                for ($i = 0; $i < $count; $i++) {
                    $keys[$i] .= "\0n";
                }

                continue;
            }

            // Rows::column() refuses a column the batch lacks, naming it
            $column = array_key_exists($position, $this->types)
                ? $equality->cast($rows->column($name), $this->types[$position], $name)
                : $rows->column($name);

            // tagged inline, not through a call: this runs once per value on the groupBy and dropDuplicates path
            // @mago-ignore analysis:mixed-assignment
            foreach ($comparable->equality($column) as $i => $value) {
                if ($value === null) {
                    $keys[$i] .= "\0n";
                } elseif (is_int($value)) {
                    $keys[$i] .= "\0i" . $value;
                } elseif (is_string($value)) {
                    $keys[$i] .= "\0s" . strlen($value) . ':' . $value;
                } elseif (is_float($value)) {
                    // -0.0 === 0.0 but serializes as "-0"; serialize(), unlike a cast, keeps every digit
                    $keys[$i] .= "\0d" . ($value === 0.0 ? '0' : serialize($value));
                } elseif (is_bool($value)) {
                    $keys[$i] .= $value ? "\0t" : "\0f";
                } else {
                    $keys[$i] .= "\0z" . serialize($value);
                }
            }
        }

        return $keys;
    }
}

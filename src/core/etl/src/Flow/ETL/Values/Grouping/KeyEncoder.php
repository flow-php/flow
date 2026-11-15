<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Grouping;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\ComparableValues;
use Flow\ETL\Values\Column\ValueEquality;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;

use function array_fill;
use function array_key_exists;
use function count;
use function hash;
use function hexdec;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function serialize;
use function strlen;
use function substr;

/**
 * Equal keys exactly when ComparableValues::equality() holds; every value is type-tagged.
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
     * @return list<Column> the key columns of $rows, cast as of() casts them
     */
    public function columns(Rows $rows, Backend $backend): array
    {
        $equality = new ValueEquality();
        $columns = [];

        foreach ($this->columns as $position => $name) {
            $declared = $this->declared?->findDefinition($name);

            if ($rows->schema()->findDefinition($name) === null && $declared?->isNullable() === true) {
                $columns[] = $backend->constant($declared, null, $rows->count());

                continue;
            }

            $columns[] = array_key_exists($position, $this->types)
                ? $equality->cast($rows->column($name), $this->types[$position], $name)
                : $rows->column($name);
        }

        return $columns;
    }

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
            self::tag(
                $keys,
                array_key_exists($position, $this->types)
                    ? $equality->cast($rows->column($name), $this->types[$position], $name)
                    : $rows->column($name),
            );
        }

        return $keys;
    }

    /**
     * @param list<Column> $columns
     *
     * @return list<string> one key per row, the keys of() gives for the same columns
     */
    public static function ofColumns(array $columns, int $count): array
    {
        if ($count < 1) {
            return [];
        }

        $keys = array_fill(0, $count, '');

        foreach ($columns as $column) {
            self::tag($keys, $column);
        }

        return $keys;
    }

    public static function bucketOf(string $key, int $buckets, int $hashOffset): int
    {
        return (int) hexdec(substr(hash('xxh128', $key), $hashOffset, 8)) % $buckets;
    }

    /**
     * @param list<string> $keys
     */
    private static function tag(array &$keys, Column $column): void
    {
        // tagged inline, not through a call per value: this runs once per value on the groupBy and dropDuplicates path
        // @mago-ignore analysis:mixed-assignment
        foreach ((new ComparableValues())->equality($column) as $i => $value) {
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
}

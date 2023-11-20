<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

final class MapValue
{
    private function __construct(public readonly Column $value)
    {
    }

    public static function boolean(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::boolean('value')->makeRequired());
        }

        return new self(FlatColumn::boolean('value'));
    }

    public static function date(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::date('value')->makeRequired());
        }

        return new self(FlatColumn::date('value'));
    }

    public static function datetime(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::dateTime('value')->makeRequired());
        }

        return new self(FlatColumn::dateTime('value'));
    }

    public static function decimal(int $precision, int $scale, bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::decimal('value', $scale, $precision)->makeRequired());
        }

        return new self(FlatColumn::decimal('value', $scale, $precision));
    }

    public static function double(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::double('value')->makeRequired());
        }

        return new self(FlatColumn::double('value'));
    }

    public static function float(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::float('value')->makeRequired());
        }

        return new self(FlatColumn::float('value'));
    }

    public static function int32(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::int32('value')->makeRequired());
        }

        return new self(FlatColumn::int32('value'));
    }

    public static function int64(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::int64('value')->makeRequired());
        }

        return new self(FlatColumn::int64('value'));
    }

    public static function list(ListElement $element, bool $required = false) : self
    {
        if ($required) {
            return new self(NestedColumn::list('value', $element)->makeRequired());
        }

        return new self(NestedColumn::list('value', $element));
    }

    public static function map(MapKey $key, self $map, bool $required = false) : self
    {
        if ($required) {
            return new self(NestedColumn::map('value', $key, $map)->makeRequired());
        }

        return new self(NestedColumn::map('value', $key, $map));
    }

    public static function string(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::string('value')->makeRequired());
        }

        return new self(FlatColumn::string('value'));
    }

    /**
     * @param array<Column> $columns
     */
    public static function structure(array $columns, bool $required = false) : self
    {
        if ($required) {
            return new self(NestedColumn::create('value', $columns)->makeRequired());
        }

        return new self(NestedColumn::create('value', $columns));
    }

    public static function time(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::time('value')->makeRequired());
        }

        return new self(FlatColumn::time('value'));
    }
}

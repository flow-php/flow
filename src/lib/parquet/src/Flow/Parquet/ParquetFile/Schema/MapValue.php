<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

final class MapValue
{
    private function __construct(public readonly Column $value)
    {
    }

    public static function boolean() : self
    {
        return new self(FlatColumn::boolean('value'));
    }

    public static function date() : self
    {
        return new self(FlatColumn::date('value'));
    }

    public static function datetime() : self
    {
        return new self(FlatColumn::dateTime('value'));
    }

    public static function decimal(int $precision, int $scale) : self
    {
        return new self(FlatColumn::decimal('value', $scale, $precision));
    }

    public static function double() : self
    {
        return new self(FlatColumn::double('value'));
    }

    public static function float() : self
    {
        return new self(FlatColumn::float('value'));
    }

    public static function int32() : self
    {
        return new self(FlatColumn::int32('value'));
    }

    public static function int64() : self
    {
        return new self(FlatColumn::int64('value'));
    }

    public static function list(ListElement $element) : self
    {
        return new self(NestedColumn::list('value', $element));
    }

    public static function map(MapKey $key, self $map) : self
    {
        return new self(NestedColumn::map('value', $key, $map));
    }

    public static function string() : self
    {
        return new self(FlatColumn::string('value'));
    }

    /**
     * @param array<Column> $columns
     */
    public static function structure(array $columns) : self
    {
        return new self(NestedColumn::create('value', $columns));
    }

    public static function time() : self
    {
        return new self(FlatColumn::time('value'));
    }
}

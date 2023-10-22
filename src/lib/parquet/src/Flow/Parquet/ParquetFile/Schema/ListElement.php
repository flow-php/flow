<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

final class ListElement
{
    private function __construct(public readonly Column $element)
    {
    }

    public static function boolean() : self
    {
        return new self(FlatColumn::boolean('element'));
    }

    public static function date() : self
    {
        return new self(FlatColumn::date('element'));
    }

    public static function datetime() : self
    {
        return new self(FlatColumn::dateTime('element'));
    }

    public static function decimal(int $precision, int $scale) : self
    {
        return new self(FlatColumn::decimal('element', $precision, $scale));
    }

    public static function double() : self
    {
        return new self(FlatColumn::double('element'));
    }

    public static function float() : self
    {
        return new self(FlatColumn::float('element'));
    }

    public static function int32() : self
    {
        return new self(FlatColumn::int32('element'));
    }

    public static function int64() : self
    {
        return new self(FlatColumn::int64('element'));
    }

    public static function list(self $element) : self
    {
        return new self(NestedColumn::list('element', $element));
    }

    public static function map(MapKey $key, MapValue $value) : self
    {
        return new self(NestedColumn::map('element', $key, $value));
    }

    public static function string() : self
    {
        return new self(FlatColumn::string('element'));
    }

    /**
     * @param array<Column> $columns
     */
    public static function structure(array $columns) : self
    {
        return new self(NestedColumn::create('element', $columns));
    }

    public static function time() : self
    {
        return new self(FlatColumn::time('element'));
    }
}

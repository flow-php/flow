<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

final class ListElement
{
    private function __construct(public readonly Column $element)
    {
    }

    public static function boolean(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::boolean('element')->makeRequired());
        }

        return new self(FlatColumn::boolean('element'));
    }

    public static function date(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::date('element')->makeRequired());
        }

        return new self(FlatColumn::date('element'));
    }

    public static function datetime(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::dateTime('element')->makeRequired());
        }

        return new self(FlatColumn::dateTime('element'));
    }

    public static function decimal(int $precision, int $scale, bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::decimal('element', $scale, $precision)->makeRequired());
        }

        return new self(FlatColumn::decimal('element', $scale, $precision));
    }

    public static function double(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::double('element')->makeRequired());
        }

        return new self(FlatColumn::double('element'));
    }

    public static function float(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::float('element')->makeRequired());
        }

        return new self(FlatColumn::float('element'));
    }

    public static function int32(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::int32('element')->makeRequired());
        }

        return new self(FlatColumn::int32('element'));
    }

    public static function int64(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::int64('element')->makeRequired());
        }

        return new self(FlatColumn::int64('element'));
    }

    public static function json(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::json('element')->makeRequired());
        }

        return new self(FlatColumn::json('element'));
    }

    public static function list(self $element, bool $required = false) : self
    {
        if ($required) {
            return new self(NestedColumn::list('element', $element)->makeRequired());
        }

        return new self(NestedColumn::list('element', $element));
    }

    public static function map(MapKey $key, MapValue $value, bool $required = false) : self
    {
        if ($required) {
            return new self(NestedColumn::map('element', $key, $value)->makeRequired());
        }

        return new self(NestedColumn::map('element', $key, $value));
    }

    public static function string(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::string('element')->makeRequired());
        }

        return new self(FlatColumn::string('element'));
    }

    /**
     * @param array<Column> $columns
     */
    public static function structure(array $columns, bool $required = false) : self
    {
        if ($required) {
            return new self(NestedColumn::create('element', $columns)->makeRequired());
        }

        return new self(NestedColumn::create('element', $columns));
    }

    public static function time(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::time('element')->makeRequired());
        }

        return new self(FlatColumn::time('element'));
    }

    public static function uuid(bool $required = false) : self
    {
        if ($required) {
            return new self(FlatColumn::uuid('element')->makeRequired());
        }

        return new self(FlatColumn::uuid('element'));
    }
}

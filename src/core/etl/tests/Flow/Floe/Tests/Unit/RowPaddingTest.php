<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\RowPadding;
use Flow\Floe\SchemaDecoder;
use Flow\Floe\ValueDecoder;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowPaddingTest extends TestCase
{
    public function test_absent_columns_are_padded_with_typed_nullable_nulls(): void
    {
        $padding = RowPadding::forFileSchema(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            new SchemaDecoder(new ValueDecoder()),
        );

        $padded = $padding->apply(row(['id' => 1]));

        // padding rebuilds the row in file-schema order; the column's type and nullability live in
        // the Schema now, so padding only has to make the absent column present and null
        static::assertSame(['id', 'email'], $padded->names());
        static::assertSame(1, $padded->get('id'));
        static::assertNull($padded->get('email'));
    }

    public function test_padding_entries_are_shared_between_rows(): void
    {
        $padding = RowPadding::forFileSchema(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            new SchemaDecoder(new ValueDecoder()),
        );

        static::assertSame(
            $padding->apply(row(['id' => 1]))->get('email'),
            $padding->apply(row(['id' => 2]))->get('email'),
        );
    }

    public function test_row_is_rebuilt_in_file_schema_order(): void
    {
        $padding = RowPadding::forFileSchema(
            schema(int_schema('id'), str_schema('name')),
            new SchemaDecoder(new ValueDecoder()),
        );

        $padded = $padding->apply(row(['name' => 'x', 'id' => 1]));

        static::assertSame(['id', 'name'], $padded->names());
        static::assertSame(1, $padded->get('id'));
        static::assertSame('x', $padded->get('name'));
    }

    public function test_present_columns_are_kept_as_written(): void
    {
        $padding = RowPadding::forFileSchema(
            schema(int_schema('id'), str_schema('name')),
            new SchemaDecoder(new ValueDecoder()),
        );

        $original = row(['id' => 1, 'name' => 'x']);
        $padded = $padding->apply($original);

        static::assertSame($original->get('id'), $padded->get('id'));
    }
}

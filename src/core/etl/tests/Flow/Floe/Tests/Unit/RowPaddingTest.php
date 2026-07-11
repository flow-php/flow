<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Schema\Metadata;
use Flow\Floe\EntryInstantiator;
use Flow\Floe\RowPadding;
use Flow\Floe\SchemaDecoder;
use Flow\Floe\ValueDecoder;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\str_schema;

final class RowPaddingTest extends TestCase
{
    public function test_absent_columns_are_padded_with_from_null_entries(): void
    {
        $padding = RowPadding::forFileSchema(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()),
        );

        $padded = $padding->apply(row(int_entry('id', 1)));
        $email = $padded->get('email');

        static::assertSame(['id', 'email'], $padded->entries()->names());
        static::assertSame(1, $padded->get('id')->value());
        static::assertNull($email->value());
        static::assertTrue($email->definition()->isNullable());
        static::assertTrue($email->definition()->metadata()->has(Metadata::FROM_NULL));
    }

    public function test_padding_entries_are_shared_between_rows(): void
    {
        $padding = RowPadding::forFileSchema(
            schema(int_schema('id'), str_schema('email', nullable: true)),
            new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()),
        );

        static::assertSame(
            $padding->apply(row(int_entry('id', 1)))->get('email'),
            $padding->apply(row(int_entry('id', 2)))->get('email'),
        );
    }

    public function test_row_is_rebuilt_in_file_schema_order(): void
    {
        $padding = RowPadding::forFileSchema(
            schema(int_schema('id'), str_schema('name')),
            new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()),
        );

        $padded = $padding->apply(row(str_entry('name', 'x'), int_entry('id', 1)));

        static::assertSame(['id', 'name'], $padded->entries()->names());
        static::assertSame(1, $padded->get('id')->value());
        static::assertSame('x', $padded->get('name')->value());
    }

    public function test_present_columns_are_kept_as_written(): void
    {
        $padding = RowPadding::forFileSchema(
            schema(int_schema('id'), str_schema('name')),
            new SchemaDecoder(new ValueDecoder(), new EntryInstantiator()),
        );

        $original = row(int_entry('id', 1), str_entry('name', 'x'));
        $padded = $padding->apply($original);

        static::assertSame($original->get('id'), $padded->get('id'));
    }
}

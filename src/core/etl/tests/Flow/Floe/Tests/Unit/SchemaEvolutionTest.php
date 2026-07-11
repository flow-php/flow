<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\Exception\IncompatibleSchemaException;
use Flow\Floe\SchemaEvolution;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SchemaEvolutionTest extends TestCase
{
    public function test_identical_schema_is_valid(): void
    {
        $this->expectNotToPerformAssertions();

        (new SchemaEvolution())->validate(
            schema(int_schema('id'), str_schema('name')),
            schema(int_schema('id'), str_schema('name')),
        );
    }

    public function test_narrowing_nullable_column_to_non_nullable_is_valid(): void
    {
        $this->expectNotToPerformAssertions();

        (new SchemaEvolution())->validate(schema(str_schema('name', nullable: true)), schema(str_schema('name')));
    }

    public function test_new_non_nullable_column_throws(): void
    {
        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('adds new column "email" which must be nullable');

        (new SchemaEvolution())->validate(schema(int_schema('id')), schema(int_schema('id'), str_schema('email')));
    }

    public function test_new_nullable_column_is_valid(): void
    {
        $this->expectNotToPerformAssertions();

        (new SchemaEvolution())->validate(
            schema(int_schema('id')),
            schema(int_schema('id'), str_schema('email', nullable: true)),
        );
    }

    public function test_nullable_incoming_column_on_non_nullable_file_column_throws(): void
    {
        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('changes column "id"');

        (new SchemaEvolution())->validate(schema(int_schema('id')), schema(int_schema('id', nullable: true)));
    }

    public function test_omitting_non_nullable_column_throws(): void
    {
        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('omits column "id" which is not nullable');

        (new SchemaEvolution())->validate(
            schema(int_schema('id'), str_schema('name', nullable: true)),
            schema(str_schema('name', nullable: true)),
        );
    }

    public function test_omitting_nullable_column_is_valid(): void
    {
        $this->expectNotToPerformAssertions();

        (new SchemaEvolution())->validate(
            schema(int_schema('id'), str_schema('name', nullable: true)),
            schema(int_schema('id')),
        );
    }

    public function test_type_change_throws(): void
    {
        $this->expectException(IncompatibleSchemaException::class);
        $this->expectExceptionMessage('changes column "id" from integer to string');

        (new SchemaEvolution())->validate(schema(int_schema('id')), schema(str_schema('id')));
    }
}

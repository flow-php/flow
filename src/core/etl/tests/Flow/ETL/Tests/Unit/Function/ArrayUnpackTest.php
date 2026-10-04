<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArrayUnpack;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayUnpackTest extends FlowTestCase
{
    public function test_array_unpack(): void
    {
        // the StructureType column returns() declares: the undeclared "array" field is not unpacked
        static::assertSame(
            [
                'status' => 'PENDING',
                'enabled' => true,
            ],
            (new FunctionContext(flow_context()))->eval(
                new ArrayUnpack(ref('array_entry'), schema(str_schema('status'), bool_schema('enabled'))),
                [
                    'id' => 1,
                    'array_entry' => [
                        'status' => 'PENDING',
                        'enabled' => true,
                        'array' => ['foo' => 'bar'],
                    ],
                ],
                schema(
                    int_schema('id'),
                    structure_schema('array_entry', type_structure([
                        'status' => type_string(),
                        'enabled' => type_boolean(),
                        'array' => type_structure(['foo' => type_string()]),
                    ])),
                ),
            ),
        );
    }

    public function test_array_unpack_with_null_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('array_unpack() requires a non-null array');

        (new FunctionContext(flow_context(config())))->eval(
            new ArrayUnpack(ref('array_entry'), schema(str_schema('status'))),
            [
                'id' => 1,
                'array_entry' => null,
            ],
            schema(int_schema('id'), str_schema('array_entry', nullable: true)),
        );
    }

    public function test_it_refuses_an_empty_schema(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('array_unpack() requires at least one declared column');

        new ArrayUnpack(ref('array_entry'), schema());
    }

    public function test_returns_the_declared_structure_with_every_column_nullable(): void
    {
        static::assertEquals(
            type_structure([
                'status' => structure_element('status', type_optional(type_string())),
                'enabled' => structure_element('enabled', type_optional(type_boolean())),
            ]),
            (new ArrayUnpack(ref('array_entry'), schema(str_schema('status'), bool_schema('enabled'))))->returns(),
        );
    }

    public function test_the_declared_columns_are_readable_through_returns(): void
    {
        static::assertEquals(
            type_structure(['payload' => structure_element('payload', type_optional(type_json()))]),
            (new ArrayUnpack(ref('array_entry'), schema(json_schema('payload'))))->returns(),
        );
    }
}

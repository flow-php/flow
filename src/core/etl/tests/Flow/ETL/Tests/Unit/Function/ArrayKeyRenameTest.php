<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ArrayDot\Exception\InvalidPathException;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_key_rename;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayKeyRenameTest extends FlowTestCase
{
    public function test_array_key_rename_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());

        (new FunctionContext($context))->eval(
            array_key_rename(ref('integer_entry'), 'invalid_path', 'new_name'),
            ['integer_entry' => 1],
            schema(int_schema('integer_entry')),
        );
    }

    public function test_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        (new FunctionContext(flow_context()))->eval(
            array_key_rename(ref('integer_entry'), 'invalid_path', 'new_name'),
            ['integer_entry' => 1],
            schema(int_schema('integer_entry')),
        );
    }

    public function test_renames_array_entry_keys_in_multiple_array_entry(): void
    {
        static::assertEquals(
            [
                'first_name' => 'John',
                'last' => 'Snow',
            ],
            (new FunctionContext(flow_context()))->eval(
                array_key_rename(ref('customer'), 'first', 'first_name'),
                [
                    'customer' => [
                        'first' => 'John',
                        'last' => 'Snow',
                    ],
                    'shipping' => [
                        'address' => [
                            'line' => '3644 Clement Street',
                            'city' => 'Atalanta',
                        ],
                        'estimated_delivery_date' => '2023-04-01T10:00:00+00:00',
                    ],
                ],
                schema(
                    structure_schema('customer', type_structure(['first' => type_string(), 'last' => type_string()])),
                    structure_schema('shipping', type_structure([
                        'address' => type_structure(['line' => type_string(), 'city' => type_string()]),
                        'estimated_delivery_date' => type_string(),
                    ])),
                ),
            ),
        );

        static::assertEquals(
            [
                'address' => [
                    'street' => '3644 Clement Street',
                    'city' => 'Atalanta',
                ],
                'estimated_delivery_date' => '2023-04-01T10:00:00+00:00',
            ],
            (new FunctionContext(flow_context()))->eval(
                array_key_rename(ref('shipping'), 'address.line', 'street'),
                [
                    'customer' => [
                        'first' => 'John',
                        'last' => 'Snow',
                    ],
                    'shipping' => [
                        'address' => [
                            'line' => '3644 Clement Street',
                            'city' => 'Atalanta',
                        ],
                        'estimated_delivery_date' => '2023-04-01T10:00:00+00:00',
                    ],
                ],
                schema(
                    structure_schema('customer', type_structure(['first' => type_string(), 'last' => type_string()])),
                    structure_schema('shipping', type_structure([
                        'address' => type_structure(['line' => type_string(), 'city' => type_string()]),
                        'estimated_delivery_date' => type_string(),
                    ])),
                ),
            ),
        );
    }

    public function test_renames_array_entry_keys_in_single_array_entry(): void
    {
        $rows = RowsMother::arrayEntry();

        static::assertEquals(
            [
                'id' => 1,
                'status' => 'PENDING',
                'enabled' => true,
                'array' => ['new_name' => 'bar'],
            ],
            (new FunctionContext(flow_context()))->eval(
                array_key_rename(ref('array_entry'), 'array.foo', 'new_name'),
                $rows->values(0),
                $rows->schema(),
            ),
        );
    }

    public function test_throws_exception_for_invalid_path(): void
    {
        $rows = RowsMother::arrayEntry();

        try {
            (new FunctionContext(flow_context()))->eval(
                array_key_rename(ref('array_entry'), 'invalid_path', 'new_name'),
                $rows->values(0),
                $rows->schema(),
            );
            static::fail('expected an EvaluationException');
        } catch (EvaluationException $e) {
            static::assertInstanceOf(InvalidPathException::class, $e->getPrevious());
            static::assertStringContainsString('Path "invalid_path" does not exists in array ', $e->getMessage());
        }
    }
}

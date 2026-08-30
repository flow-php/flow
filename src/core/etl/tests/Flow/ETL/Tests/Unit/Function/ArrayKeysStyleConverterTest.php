<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_keys_style_convert;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ArrayKeysStyleConverterTest extends FlowTestCase
{
    public function test_for_invalid_style(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unrecognized style invalid, please use one of following:');

        $row = row(json_entry('invalid_entry', []));

        array_keys_style_convert(ref('invalid_entry'), 'invalid')->eval($row, flow_context());
    }

    public function test_for_not_array_entry(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $row = row(int_entry('invalid_entry', 1));

        array_keys_style_convert(ref('invalid_entry'), 'snake')->eval($row, flow_context());
    }

    public function test_transforms_case_style_for_all_keys_in_array_entry(): void
    {
        $row = row(json_entry('arrayEntry', [
            'itemId' => 1,
            'itemStatus' => 'PENDING',
            'itemEnabled' => true,
            'itemVariants' => [
                'variantStatuses' => [
                    [
                        'statusId' => 1000,
                        'statusName' => 'NEW',
                    ],
                    [
                        'statusId' => 2000,
                        'statusName' => 'ACTIVE',
                    ],
                ],
                'variantName' => 'Variant Name',
            ],
        ]));

        static::assertEquals(
            [
                'item_id' => 1,
                'item_status' => 'PENDING',
                'item_enabled' => true,
                'item_variants' => [
                    'variant_statuses' => [
                        [
                            'status_id' => 1000,
                            'status_name' => 'NEW',
                        ],
                        [
                            'status_id' => 2000,
                            'status_name' => 'ACTIVE',
                        ],
                    ],
                    'variant_name' => 'Variant Name',
                ],
            ],
            array_keys_style_convert(ref('arrayEntry'), 'snake')->eval($row, flow_context()),
        );
    }

    public function test_colliding_converted_field_names_are_refused_instead_of_silently_dropped(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage(
            'fields "foo_bar" and "fooBar" of "structure{foo_bar: integer, fooBar: string}" both convert to "fooBar"',
        );

        (new ReferenceResolver())
            ->resolve(
                array_keys_style_convert(ref('structure'), StringStyles::CAMEL),
                schema(structure_schema('structure', type_structure([
                    'foo_bar' => type_integer(),
                    'fooBar' => type_string(),
                ]))),
            )
            ->returns();
    }
}

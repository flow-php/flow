<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Unit;

use CmsIg\Seal\Schema\Field;
use Flow\ETL\Adapter\Seal\SealMetadata;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function Flow\ETL\Adapter\Seal\seal_schema_to_flow;
use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class SchemaConverterTest extends FlowTestCase
{
    public function test_converting_flow_schema_to_a_seal_index(): void
    {
        $sealSchema = to_seal_schema(
            schema(
                str_schema('id'),
                str_schema('title'),
                int_schema('rating'),
                float_schema('price'),
                bool_schema('active'),
                datetime_schema('created_at'),
                list_schema('tags', type_list(type_string())),
                list_schema('items', type_list(type_structure(['sku' => type_string(), 'quantity' => type_integer()]))),
                structure_schema('author', type_structure(['name' => type_string()])),
            ),
            'blog',
            'id',
        );

        $fields = $sealSchema->indexes['blog']->fields;

        static::assertInstanceOf(Field\IdentifierField::class, $fields['id']);
        static::assertInstanceOf(Field\TextField::class, $fields['title']);
        static::assertInstanceOf(Field\IntegerField::class, $fields['rating']);
        static::assertInstanceOf(Field\FloatField::class, $fields['price']);
        static::assertInstanceOf(Field\BooleanField::class, $fields['active']);
        static::assertInstanceOf(Field\DateTimeField::class, $fields['created_at']);
        static::assertInstanceOf(Field\TextField::class, $fields['tags']);
        static::assertTrue($fields['tags']->multiple);
        static::assertInstanceOf(Field\ObjectField::class, $fields['items']);
        static::assertTrue($fields['items']->multiple);
        static::assertInstanceOf(Field\ObjectField::class, $fields['author']);
        static::assertFalse($fields['author']->multiple);
    }

    public function test_identifier_can_be_marked_with_metadata(): void
    {
        $sealSchema = to_seal_schema(
            schema(str_schema('uuid', metadata: SealMetadata::identifier()), str_schema('name')),
            'index',
        );

        static::assertInstanceOf(Field\IdentifierField::class, $sealSchema->indexes['index']->fields['uuid']);
    }

    public function test_reverse_converts_a_seal_index_back_to_a_flow_schema(): void
    {
        $flowSchema = seal_schema_to_flow(to_seal_schema(
            schema(
                str_schema('id'),
                int_schema('count'),
                list_schema('tags', type_list(type_string())),
                structure_schema('author', type_structure(['name' => type_string()])),
            ),
            'index',
            'id',
        ));

        static::assertInstanceOf(StringType::class, $flowSchema->get('id')->type());
        static::assertFalse($flowSchema->get('id')->isNullable());
        static::assertInstanceOf(IntegerType::class, $flowSchema->get('count')->type());
        static::assertInstanceOf(ListType::class, $flowSchema->get('tags')->type());
        static::assertInstanceOf(StructureType::class, $flowSchema->get('author')->type());
    }

    public function test_throws_when_no_identifier_is_provided(): void
    {
        $this->expectException(RuntimeException::class);

        to_seal_schema(schema(str_schema('name')), 'index');
    }

    public function test_throws_when_structure_has_optional_elements(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Seal schema does not support structure optional elements, given: structure{name: string, nickname?: string}',
        );

        to_seal_schema(
            schema(
                str_schema('id'),
                structure_schema('author', type_structure([
                    'name' => type_string(),
                    'nickname' => structure_element('nickname', type_string(), optional: true),
                ])),
            ),
            'index',
        );
    }

    public function test_structure_with_an_interleaved_optional_element_throws(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Seal schema does not support structure optional elements, given: structure{nickname?: string, name: string}',
        );

        to_seal_schema(
            schema(structure_schema('author', type_structure([
                'nickname' => structure_element('nickname', type_string(), optional: true),
                'name' => type_string(),
            ]))),
            'index',
        );
    }

    public function test_using_metadata_to_override_default_field_flags(): void
    {
        $sealSchema = to_seal_schema(
            schema(
                str_schema('id'),
                str_schema('title', metadata: SealMetadata::filterable()->merge(SealMetadata::sortable())),
            ),
            'blog',
            'id',
        );

        /** @var Field\TextField $title */
        $title = $sealSchema->indexes['blog']->fields['title'];

        static::assertTrue($title->filterable);
        static::assertTrue($title->sortable);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\Row\Schema\Constraint;
use Flow\ETL\Row\Schema\Definition;
use PHPUnit\Framework\TestCase;

final class DefinitionTest extends TestCase
{
    public function test_creating_definition_without_class() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Entry class "DateTimeInterface" must implement "Flow\ETL\Row\Entry"');

        new Definition('name', \DateTimeInterface::class, type_datetime());
    }

    public function test_equals_but_different_constraints() : void
    {
        $def = Definition::list('list', new ListType(ListElement::integer()));

        $this->assertFalse(
            $def->isEqual(
                Definition::list('list', new ListType(ListElement::string()))
            )
        );
    }

    public function test_equals_types_and_constraints() : void
    {
        $def = Definition::list('list', new ListType(ListElement::integer()));

        $this->assertTrue(
            $def->isEqual(
                Definition::list('list', new ListType(ListElement::integer()))
            )
        );
    }

    public function test_matches_when_constraint_satisfied_and_everything_else_matches() : void
    {
        $constraint = $this->createMock(Constraint::class);
        $constraint->expects($this->any())
            ->method('isSatisfiedBy')
            ->willReturn(true);

        $def = Definition::integer('test', false, $constraint);

        $this->assertTrue($def->matches(int_entry('test', 1)));
    }

    public function test_matches_when_nullable_and_name_matches() : void
    {
        $def = Definition::integer('test', $nullable = true);

        $this->assertTrue($def->matches(null_entry('test')));
    }

    public function test_matches_when_type_and_name_match() : void
    {
        $def = Definition::integer('test');

        $this->assertTrue($def->matches(int_entry('test', 1)));
    }

    public function test_merge_definitions_with_both_side_constraints() : void
    {
        $this->assertEquals(
            Definition::integer(
                'id',
                true,
                new Constraint\Any(
                    new Constraint\SameAs(1),
                    new Constraint\SameAs('one')
                )
            ),
            Definition::integer('id', false, new Constraint\SameAs(1))
                ->merge(Definition::integer('id', true, new Constraint\SameAs('one')))
        );
    }

    public function test_merge_definitions_with_left_side_constraints() : void
    {
        $this->assertEquals(
            Definition::integer(
                'id',
                true,
                new Constraint\SameAs(1)
            ),
            Definition::integer('id', false, new Constraint\SameAs(1))->merge(Definition::integer('id', true))
        );
    }

    public function test_merge_definitions_with_right_side_constraints() : void
    {
        $this->assertEquals(
            Definition::integer(
                'id',
                true,
                new Constraint\SameAs(2)
            )->nullable(),
            Definition::integer('id')->merge(Definition::integer('id', true, new Constraint\SameAs(2)))
        );
    }

    public function test_merge_definitions_without_constraints() : void
    {
        $this->assertEquals(
            Definition::integer('id', true)->nullable(),
            Definition::integer('id')->merge(Definition::integer('id', true))
        );
    }

    public function test_merging_anything_and_string() : void
    {
        $this->assertEquals(
            Definition::string('id', true),
            Definition::integer('id', false)->merge(Definition::string('id', true))
        );
        $this->assertEquals(
            Definition::string('id', true),
            Definition::float('id', false)->merge(Definition::string('id', true))
        );
        $this->assertEquals(
            Definition::string('id', true),
            Definition::boolean('id', false)->merge(Definition::string('id', true))
        );
        $this->assertEquals(
            Definition::string('id', true),
            Definition::dateTime('id', false)->merge(Definition::string('id', true))
        );
    }

    public function test_merging_anything_with_null() : void
    {
        $this->assertEquals(
            Definition::string('id', true),
            Definition::string('id', false)->merge(Definition::null('id'))
        );
        $this->assertEquals(
            Definition::dateTime('datetime', true),
            Definition::dateTime('datetime', false)->merge(Definition::null('datetime'))
        );
        $this->assertEquals(
            Definition::integer('id', true),
            Definition::integer('id', false)->merge(Definition::null('id'))
        );
        $this->assertEquals(
            Definition::float('id', true),
            Definition::float('id', false)->merge(Definition::null('id'))
        );
    }

    public function test_merging_different_entries() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, int and string');

        Definition::integer('int')->merge(Definition::string('string'));
    }

    public function test_merging_numeric_types() : void
    {
        $this->assertEquals(
            Definition::float('id', true),
            Definition::integer('id', false)->merge(Definition::float('id', true))
        );
        $this->assertEquals(
            Definition::float('id', true),
            Definition::float('id', false)->merge(Definition::integer('id', true))
        );
    }

    public function test_merging_two_different_lists() : void
    {
        $this->assertEquals(
            Definition::array('list'),
            Definition::list('list', type_list(type_string()))->merge(Definition::list('list', type_list(type_int())))
        );
    }

    public function test_merging_two_different_maps() : void
    {
        $this->assertEquals(
            Definition::array('map'),
            Definition::map('map', type_map(type_string(), type_string()))->merge(Definition::map('map', type_map(type_string(), type_int())))
        );
    }

    public function test_merging_two_different_structures() : void
    {
        $this->assertEquals(
            Definition::array('structure'),
            Definition::structure(
                'structure',
                struct_type([
                    struct_element('street', type_string()),
                    struct_element('city', type_string()),
                ])
            )->merge(
                Definition::structure(
                    'structure',
                    struct_type([
                        struct_element('street', type_string()),
                        struct_element('city', type_int()),
                    ])
                )
            )
        );
    }

    public function test_merging_two_same_lists() : void
    {
        $this->assertEquals(
            Definition::list('list', type_list(type_int())),
            Definition::list('list', type_list(type_int()))->merge(Definition::list('list', type_list(type_int())))
        );
    }

    public function test_merging_two_same_maps() : void
    {
        $this->assertEquals(
            Definition::map('map', type_map(type_string(), type_string())),
            Definition::map('map', type_map(type_string(), type_string()))->merge(Definition::map('map', type_map(type_string(), type_string())))
        );
    }

    public function test_not_matches_when_constraint_not_satisfied() : void
    {
        $constraint = $this->createMock(Constraint::class);
        $constraint->expects($this->any())
            ->method('isSatisfiedBy')
            ->willReturn(false);

        $def = Definition::integer('test', false, $constraint);

        $this->assertFalse($def->matches(int_entry('test', 1)));
    }

    public function test_not_matches_when_not_nullable_name_matches_but_null_given() : void
    {
        $def = Definition::integer('test', $nullable = false);

        $this->assertFalse($def->matches(null_entry('test')));
    }

    public function test_not_matches_when_type_does_not_match() : void
    {
        $def = Definition::integer('test');

        $this->assertFalse($def->matches(str_entry('test', 'test')));
    }

    public function test_not_matches_when_type_name_not_match() : void
    {
        $def = Definition::integer('test');

        $this->assertFalse($def->matches(int_entry('not-test', 1)));
    }

    public function test_structure_definition_metadata() : void
    {
        $address = struct_entry(
            'address',
            [
                'street' => 'street',
                'city' => 'city',
                'location' => ['lat' => 1.0, 'lng' => 1.0],
            ],
            struct_type([
                struct_element('street', type_string()),
                struct_element('city', type_string()),
                struct_element(
                    'location',
                    struct_type([
                        struct_element('lat', type_float()),
                        struct_element('lng', type_float()),
                    ])
                ),
            ]),
        );

        $this->assertEquals(
            new StructureType([
                struct_element('street', type_string()),
                struct_element('city', type_string()),
                struct_element(
                    'location',
                    new StructureType([
                        struct_element('lat', type_float()),
                        struct_element('lng', type_float()),
                    ])
                ),
            ]),
            $address->definition()->type()
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;
use Flow\ETL\Row\Schema\Constraint;
use Flow\ETL\Row\Schema\Definition;
use PHPUnit\Framework\TestCase;

final class DefinitionTest extends TestCase
{
    public function test_creating_definition_without_class() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema definition must come with at least one entry class');

        new Definition('name', []);
    }

    public function test_equals() : void
    {
        $def = Definition::union('test', [IntegerEntry::class, StringEntry::class]);

        $this->assertTrue(
            $def->isEqual(
                Definition::union('test', [StringEntry::class, IntegerEntry::class])
            )
        );

        $this->assertFalse(
            $def->isEqual(
                Definition::boolean('test', false)
            )
        );
    }

    public function test_equals_but_different_constraints() : void
    {
        $def = Definition::list('list', ScalarType::integer);

        $this->assertFalse(
            $def->isEqual(
                Definition::list('list', ScalarType::string)
            )
        );
    }

    public function test_equals_types_and_constraints() : void
    {
        $def = Definition::list('list', ScalarType::integer);

        $this->assertTrue(
            $def->isEqual(
                Definition::list('list', ScalarType::integer)
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

        $this->assertTrue($def->matches(Entry::integer('test', 1)));
    }

    public function test_matches_when_nullable_and_name_matches() : void
    {
        $def = Definition::integer('test', $nullable = true);

        $this->assertTrue($def->matches(Entry::null('test')));
    }

    public function test_matches_when_type_and_name_match() : void
    {
        $def = Definition::integer('test');

        $this->assertTrue($def->matches(Entry::integer('test', 1)));
    }

    public function test_merge_definitions_with_both_side_constraints() : void
    {
        $this->assertEquals(
            Definition::union(
                'id',
                [IntegerEntry::class, StringEntry::class],
                new Constraint\Any(
                    new Constraint\SameAs(1),
                    new Constraint\SameAs('one')
                )
            )->nullable(),
            Definition::integer('id', false, new Constraint\SameAs(1))
                ->merge(Definition::string('id', true, new Constraint\SameAs('one')))
        );
    }

    public function test_merge_definitions_with_left_side_constraints() : void
    {
        $this->assertEquals(
            Definition::union('id', [StringEntry::class, IntegerEntry::class], new Constraint\SameAs(1))->nullable(),
            Definition::string('id', false, new Constraint\SameAs(1))->merge(Definition::integer('id', true))
        );
    }

    public function test_merge_definitions_with_right_side_constraints() : void
    {
        $this->assertEquals(
            Definition::union('id', [StringEntry::class, IntegerEntry::class], new Constraint\SameAs(2))->nullable(),
            Definition::string('id', false)->merge(Definition::integer('id', true, new Constraint\SameAs(2)))
        );
    }

    public function test_merge_definitions_without_constraints() : void
    {
        $this->assertEquals(
            Definition::union('id', [StringEntry::class, IntegerEntry::class])->nullable(),
            Definition::string('id', false)->merge(Definition::integer('id', true))
        );
    }

    public function test_merge_list_definitions_with_both_side_constraints() : void
    {
        $this->assertEquals(
            Definition::union(
                'list',
                [ListEntry::class],
                new Constraint\Any(
                    new Constraint\CollectionType(ScalarType::string),
                    new Constraint\CollectionType(ScalarType::integer),
                )
            ),
            Definition::list('list', ScalarType::string)
                ->merge(Definition::list('list', ScalarType::integer))
        );
    }

    public function test_merging_two_different_lists_should_give_another_list() : void
    {
        $this->assertEquals(
            new Definition(
                'list',
                [ListEntry::class, NullEntry::class],
                new Constraint\Any(new Constraint\CollectionType(ScalarType::integer), new Constraint\CollectionType(ScalarType::string))
            ),
            Definition::list('list', ScalarType::integer)
                ->merge(Definition::list('list', ScalarType::string, true))
        );
    }

    public function test_not_matches_when_constraint_not_satisfied() : void
    {
        $constraint = $this->createMock(Constraint::class);
        $constraint->expects($this->any())
            ->method('isSatisfiedBy')
            ->willReturn(false);

        $def = Definition::integer('test', false, $constraint);

        $this->assertFalse($def->matches(Entry::integer('test', 1)));
    }

    public function test_not_matches_when_not_nullable_name_matches_but_null_given() : void
    {
        $def = Definition::integer('test', $nullable = false);

        $this->assertFalse($def->matches(Entry::null('test')));
    }

    public function test_not_matches_when_type_does_not_match() : void
    {
        $def = Definition::integer('test');

        $this->assertFalse($def->matches(Entry::string('test', 'test')));
    }

    public function test_not_matches_when_type_name_not_match() : void
    {
        $def = Definition::integer('test');

        $this->assertFalse($def->matches(Entry::integer('not-test', 1)));
    }

    public function test_union_type_definition() : void
    {
        $def = Definition::union('test', [IntegerEntry::class, StringEntry::class]);

        $this->assertFalse($def->matches(Entry::integer('not-test', 1)));
        $this->assertTrue($def->matches(Entry::integer('test', 1)));
        $this->assertTrue($def->matches(Entry::string('test', 'test')));
        $this->assertFalse($def->matches(Entry::boolean('test', false)));
    }
}

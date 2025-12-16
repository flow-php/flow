<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema;

use function Flow\PostgreSql\DSL\{current_date, current_time, current_timestamp};
use Flow\PostgreSql\Protobuf\AST\{ColumnDef, ConstrType};
use Flow\PostgreSql\QueryBuilder\Expression\SQLValueFunctionExpression;
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnDefinition, DataType};

use PHPUnit\Framework\TestCase;

final class ColumnDefinitionTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_basic_column() : void
    {
        $column = ColumnDefinition::create('id', DataType::integer());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertSame('id', $ast->getColname());
        self::assertFalse($ast->getIsNotNull());
    }

    public function test_check_constraint() : void
    {
        $column = ColumnDefinition::create('age', DataType::integer())
            ->check('age > 0');

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_CHECK, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_default_with_current_date() : void
    {
        $column = ColumnDefinition::create('birth_date', DataType::date())
            ->default(current_date());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_default_with_current_time() : void
    {
        $column = ColumnDefinition::create('check_in_time', DataType::time())
            ->default(current_time());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_default_with_current_timestamp() : void
    {
        $column = ColumnDefinition::create('created_at', DataType::timestamp())
            ->default(current_timestamp());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_default_with_expression() : void
    {
        $column = ColumnDefinition::create('created_at', DataType::timestamp())
            ->default(SQLValueFunctionExpression::currentTimestamp());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_boolean_default_false() : void
    {
        $column = ColumnDefinition::create('is_active', DataType::boolean())
            ->default(false);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_boolean_default_true() : void
    {
        $column = ColumnDefinition::create('is_active', DataType::boolean())
            ->default(true);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_default_integer() : void
    {
        $column = ColumnDefinition::create('status', DataType::integer())
            ->default(0);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_default_null() : void
    {
        $column = ColumnDefinition::create('optional', DataType::text())
            ->default(null);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_default_raw_expression() : void
    {
        $column = ColumnDefinition::create('created_at', DataType::timestamp())
            ->defaultRaw('CURRENT_TIMESTAMP');

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_default_string() : void
    {
        $column = ColumnDefinition::create('name', DataType::varchar(100))
            ->default('Unknown');

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_generated_column() : void
    {
        $column = ColumnDefinition::create('full_name', DataType::text())
            ->generatedAs("first_name || ' ' || last_name");

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertSame('s', $ast->getGenerated());
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_GENERATED, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_identity_always() : void
    {
        $column = ColumnDefinition::create('id', DataType::integer())
            ->identity('ALWAYS');

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertSame('a', $ast->getIdentity());
    }

    public function test_identity_by_default() : void
    {
        $column = ColumnDefinition::create('id', DataType::integer())
            ->identity('BY DEFAULT');

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertSame('d', $ast->getIdentity());
    }

    public function test_immutability() : void
    {
        $original = ColumnDefinition::create('id', DataType::integer());
        $modified = $original->notNull();

        self::assertNotSame($original, $modified);
        self::assertCount(0, $original->toAst()->getConstraints());
        self::assertCount(1, $modified->toAst()->getConstraints());
        self::assertSame(ConstrType::CONSTR_NOTNULL, $modified->toAst()->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_multiple_constraints() : void
    {
        $column = ColumnDefinition::create('id', DataType::integer())
            ->notNull()
            ->primaryKey();

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(2, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_NOTNULL, $ast->getConstraints()[0]->getConstraint()->getContype());
        self::assertSame(ConstrType::CONSTR_PRIMARY, $ast->getConstraints()[1]->getConstraint()->getContype());
    }

    public function test_not_null_column() : void
    {
        $column = ColumnDefinition::create('name', DataType::varchar(100))
            ->notNull();

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_NOTNULL, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_nullable_column() : void
    {
        $column = ColumnDefinition::create('name', DataType::varchar(100))
            ->notNull()
            ->nullable();

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(0, $ast->getConstraints());
    }

    public function test_primary_key_constraint() : void
    {
        $column = ColumnDefinition::create('id', DataType::integer())
            ->primaryKey();

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_PRIMARY, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_references_constraint() : void
    {
        $column = ColumnDefinition::create('user_id', DataType::integer())
            ->references('users', 'id');

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        self::assertSame(ConstrType::CONSTR_FOREIGN, $constraint->getContype());
        self::assertSame('users', $constraint->getPktable()->getRelname());
    }

    public function test_references_with_schema() : void
    {
        $column = ColumnDefinition::create('user_id', DataType::integer())
            ->references('users', 'id', 'public');

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        $constraint = $ast->getConstraints()[0]->getConstraint();
        self::assertSame('public', $constraint->getPktable()->getSchemaname());
        self::assertSame('users', $constraint->getPktable()->getRelname());
    }

    public function test_unique_constraint() : void
    {
        $column = ColumnDefinition::create('email', DataType::varchar(255))
            ->unique();

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_UNIQUE, $ast->getConstraints()[0]->getConstraint()->getContype());
    }
}

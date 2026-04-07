<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema;

use function Flow\PostgreSql\DSL\{col, concat, current_date, current_time, current_timestamp, gt, literal};
use Flow\PostgreSql\Protobuf\AST\{ColumnDef, ConstrType};
use Flow\PostgreSql\QueryBuilder\Expression\SQLValueFunctionExpression;
use Flow\PostgreSql\QueryBuilder\Schema\{ColumnDefinition, ColumnType};
use Flow\PostgreSql\Schema\IdentityGeneration;
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
        $column = ColumnDefinition::create('id', ColumnType::integer());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertSame('id', $ast->getColname());
        self::assertFalse($ast->getIsNotNull());
    }

    public function test_check_constraint() : void
    {
        $column = ColumnDefinition::create('age', ColumnType::integer())
            ->check(gt(col('age'), literal(0)));

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_CHECK, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_default_with_current_date() : void
    {
        $column = ColumnDefinition::create('birth_date', ColumnType::date())
            ->default(current_date());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_default_with_current_time() : void
    {
        $column = ColumnDefinition::create('check_in_time', ColumnType::time())
            ->default(current_time());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_default_with_current_timestamp() : void
    {
        $column = ColumnDefinition::create('created_at', ColumnType::timestamp())
            ->default(current_timestamp());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_default_with_expression() : void
    {
        $column = ColumnDefinition::create('created_at', ColumnType::timestamp())
            ->default(SQLValueFunctionExpression::currentTimestamp());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_boolean_default_false() : void
    {
        $column = ColumnDefinition::create('is_active', ColumnType::boolean())
            ->default(false);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_boolean_default_true() : void
    {
        $column = ColumnDefinition::create('is_active', ColumnType::boolean())
            ->default(true);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_default_integer() : void
    {
        $column = ColumnDefinition::create('status', ColumnType::integer())
            ->default(0);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_default_null() : void
    {
        $column = ColumnDefinition::create('optional', ColumnType::text())
            ->default(null);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_default_raw_expression() : void
    {
        $column = ColumnDefinition::create('created_at', ColumnType::timestamp())
            ->defaultRaw(current_timestamp());

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_column_with_default_string() : void
    {
        $column = ColumnDefinition::create('name', ColumnType::varchar(100))
            ->default('Unknown');

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_DEFAULT, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_generated_column() : void
    {
        $column = ColumnDefinition::create('full_name', ColumnType::text())
            ->generatedAs(concat(col('first_name'), literal(' '), col('last_name')));

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertSame('s', $ast->getGenerated());
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_GENERATED, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_identity_always() : void
    {
        $column = ColumnDefinition::create('id', ColumnType::integer())
            ->identity(IdentityGeneration::ALWAYS);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);

        $constraints = $ast->getConstraints();
        self::assertCount(1, $constraints);
        self::assertSame(ConstrType::CONSTR_IDENTITY, $constraints[0]->getConstraint()->getContype());
        self::assertSame('a', $constraints[0]->getConstraint()->getGeneratedWhen());
    }

    public function test_identity_by_default() : void
    {
        $column = ColumnDefinition::create('id', ColumnType::integer())
            ->identity(IdentityGeneration::BY_DEFAULT);

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);

        $constraints = $ast->getConstraints();
        self::assertCount(1, $constraints);
        self::assertSame(ConstrType::CONSTR_IDENTITY, $constraints[0]->getConstraint()->getContype());
        self::assertSame('d', $constraints[0]->getConstraint()->getGeneratedWhen());
    }

    public function test_immutability() : void
    {
        $original = ColumnDefinition::create('id', ColumnType::integer());
        $modified = $original->notNull();

        self::assertNotSame($original, $modified);
        self::assertCount(0, $original->toAst()->getConstraints());
        self::assertCount(1, $modified->toAst()->getConstraints());
        self::assertSame(ConstrType::CONSTR_NOTNULL, $modified->toAst()->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_multiple_constraints() : void
    {
        $column = ColumnDefinition::create('id', ColumnType::integer())
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
        $column = ColumnDefinition::create('name', ColumnType::varchar(100))
            ->notNull();

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_NOTNULL, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_nullable_column() : void
    {
        $column = ColumnDefinition::create('name', ColumnType::varchar(100))
            ->notNull()
            ->nullable();

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(0, $ast->getConstraints());
    }

    public function test_primary_key_constraint() : void
    {
        $column = ColumnDefinition::create('id', ColumnType::integer())
            ->primaryKey();

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_PRIMARY, $ast->getConstraints()[0]->getConstraint()->getContype());
    }

    public function test_references_constraint() : void
    {
        $column = ColumnDefinition::create('user_id', ColumnType::integer())
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
        $column = ColumnDefinition::create('user_id', ColumnType::integer())
            ->references('users', 'id', 'public');

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        $constraint = $ast->getConstraints()[0]->getConstraint();
        self::assertSame('public', $constraint->getPktable()->getSchemaname());
        self::assertSame('users', $constraint->getPktable()->getRelname());
    }

    public function test_unique_constraint() : void
    {
        $column = ColumnDefinition::create('email', ColumnType::varchar(255))
            ->unique();

        $ast = $column->toAst();

        self::assertInstanceOf(ColumnDef::class, $ast);
        self::assertCount(1, $ast->getConstraints());
        self::assertSame(ConstrType::CONSTR_UNIQUE, $ast->getConstraints()[0]->getConstraint()->getContype());
    }
}

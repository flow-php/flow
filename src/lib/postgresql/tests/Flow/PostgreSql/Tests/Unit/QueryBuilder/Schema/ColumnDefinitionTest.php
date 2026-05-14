<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema;

use Flow\PostgreSql\Protobuf\AST\ColumnDef;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\QueryBuilder\Expression\SQLValueFunctionExpression;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnDefinition;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\IdentityGeneration;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\concat;
use function Flow\PostgreSql\DSL\current_date;
use function Flow\PostgreSql\DSL\current_time;
use function Flow\PostgreSql\DSL\current_timestamp;
use function Flow\PostgreSql\DSL\gt;
use function Flow\PostgreSql\DSL\literal;

final class ColumnDefinitionTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_basic_column(): void
    {
        $column = ColumnDefinition::create('id', ColumnType::integer());

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertSame('id', $ast->getColname());
        static::assertFalse($ast->getIsNotNull());
    }

    public function test_check_constraint(): void
    {
        $column = ColumnDefinition::create('age', ColumnType::integer())->check(gt(col('age'), literal(0)));

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_CHECK, $constraint->getContype());
    }

    public function test_column_default_with_current_date(): void
    {
        $column = ColumnDefinition::create('birth_date', ColumnType::date())->default(current_date());

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_column_default_with_current_time(): void
    {
        $column = ColumnDefinition::create('check_in_time', ColumnType::time())->default(current_time());

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_column_default_with_current_timestamp(): void
    {
        $column = ColumnDefinition::create('created_at', ColumnType::timestamp())->default(current_timestamp());

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_column_default_with_expression(): void
    {
        $column = ColumnDefinition::create('created_at', ColumnType::timestamp())->default(
            SQLValueFunctionExpression::currentTimestamp(),
        );

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_column_with_boolean_default_false(): void
    {
        $column = ColumnDefinition::create('is_active', ColumnType::boolean())->default(false);

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_column_with_boolean_default_true(): void
    {
        $column = ColumnDefinition::create('is_active', ColumnType::boolean())->default(true);

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_column_with_default_integer(): void
    {
        $column = ColumnDefinition::create('status', ColumnType::integer())->default(0);

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_column_with_default_null(): void
    {
        $column = ColumnDefinition::create('optional', ColumnType::text())->default(null);

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_column_with_default_raw_expression(): void
    {
        $column = ColumnDefinition::create('created_at', ColumnType::timestamp())->defaultRaw(current_timestamp());

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_column_with_default_string(): void
    {
        $column = ColumnDefinition::create('name', ColumnType::varchar(100))->default('Unknown');

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_generated_column(): void
    {
        $column = ColumnDefinition::create('full_name', ColumnType::text())->generatedAs(concat(
            col('first_name'),
            literal(' '),
            col('last_name'),
        ));

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertSame('s', $ast->getGenerated());
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_GENERATED, $constraint->getContype());
    }

    public function test_identity_always(): void
    {
        $column = ColumnDefinition::create('id', ColumnType::integer())->identity(IdentityGeneration::ALWAYS);

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);

        $constraints = $ast->getConstraints();
        static::assertCount(1, $constraints);
        $constraint = $constraints[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_IDENTITY, $constraint->getContype());
        static::assertSame('a', $constraint->getGeneratedWhen());
    }

    public function test_identity_by_default(): void
    {
        $column = ColumnDefinition::create('id', ColumnType::integer())->identity(IdentityGeneration::BY_DEFAULT);

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);

        $constraints = $ast->getConstraints();
        static::assertCount(1, $constraints);
        $constraint = $constraints[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_IDENTITY, $constraint->getContype());
        static::assertSame('d', $constraint->getGeneratedWhen());
    }

    public function test_immutability(): void
    {
        $original = ColumnDefinition::create('id', ColumnType::integer());
        $modified = $original->notNull();

        static::assertNotSame($original, $modified);
        static::assertCount(0, $original->toAst()->getConstraints());
        static::assertCount(1, $modified->toAst()->getConstraints());
        $constraint = $modified->toAst()->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_NOTNULL, $constraint->getContype());
    }

    public function test_multiple_constraints(): void
    {
        $column = ColumnDefinition::create('id', ColumnType::integer())->notNull()->primaryKey();

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(2, $ast->getConstraints());
        $notNullConstraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($notNullConstraint);
        static::assertSame(ConstrType::CONSTR_NOTNULL, $notNullConstraint->getContype());
        $primaryConstraint = $ast->getConstraints()[1]->getConstraint();
        static::assertNotNull($primaryConstraint);
        static::assertSame(ConstrType::CONSTR_PRIMARY, $primaryConstraint->getContype());
    }

    public function test_not_null_column(): void
    {
        $column = ColumnDefinition::create('name', ColumnType::varchar(100))->notNull();

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_NOTNULL, $constraint->getContype());
    }

    public function test_nullable_column(): void
    {
        $column = ColumnDefinition::create('name', ColumnType::varchar(100))->notNull()->nullable();

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(0, $ast->getConstraints());
    }

    public function test_primary_key_constraint(): void
    {
        $column = ColumnDefinition::create('id', ColumnType::integer())->primaryKey();

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_PRIMARY, $constraint->getContype());
    }

    public function test_references_constraint(): void
    {
        $column = ColumnDefinition::create('user_id', ColumnType::integer())->references('users', 'id');

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_FOREIGN, $constraint->getContype());
        $pktable = $constraint->getPktable();
        static::assertNotNull($pktable);
        static::assertSame('users', $pktable->getRelname());
    }

    public function test_references_with_schema(): void
    {
        $column = ColumnDefinition::create('user_id', ColumnType::integer())->references('users', 'id', 'public');

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        $pktable = $constraint->getPktable();
        static::assertNotNull($pktable);
        static::assertSame('public', $pktable->getSchemaname());
        static::assertSame('users', $pktable->getRelname());
    }

    public function test_unique_constraint(): void
    {
        $column = ColumnDefinition::create('email', ColumnType::varchar(255))->unique();

        $ast = $column->toAst();

        static::assertInstanceOf(ColumnDef::class, $ast);
        static::assertCount(1, $ast->getConstraints());
        $constraint = $ast->getConstraints()[0]->getConstraint();
        static::assertNotNull($constraint);
        static::assertSame(ConstrType::CONSTR_UNIQUE, $constraint->getContype());
    }
}

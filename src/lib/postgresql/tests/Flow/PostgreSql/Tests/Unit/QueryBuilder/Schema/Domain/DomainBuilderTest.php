<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Domain;

use function Flow\PostgreSql\DSL\{alter, col, column_type_text, create, drop, literal};

use Flow\PostgreSql\Protobuf\AST\{AlterDomainStmt, ConstrType, CreateDomainStmt, DropBehavior, DropStmt, ObjectType};
use Flow\PostgreSql\QueryBuilder\Condition\OperatorCondition;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\QueryBuilder\Schema\Domain\{AlterDomainBuilder, CreateDomainBuilder, DropDomainBuilder};

use PHPUnit\Framework\TestCase;

final class DomainBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_domain_add_constraint() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->addConstraint('valid_email', new OperatorCondition(col('VALUE'), '~', literal('^.+@.+$')));

        $ast = $builder->toAst();

        self::assertSame('C', $ast->getSubtype());
        self::assertSame('valid_email', $ast->getName());
    }

    public function test_alter_domain_ast_type() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->setNotNull();

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterDomainStmt::class, $ast);
    }

    public function test_alter_domain_cascade() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->dropConstraint('valid_email')
            ->cascade();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_alter_domain_drop_constraint() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->dropConstraint('valid_email');

        $ast = $builder->toAst();

        self::assertSame('X', $ast->getSubtype());
        self::assertSame('valid_email', $ast->getName());
    }

    public function test_alter_domain_drop_constraint_cascade_to_sql() : void
    {
        self::assertSame(
            'ALTER DOMAIN email DROP CONSTRAINT valid_email CASCADE',
            alter()->domain('email')
                ->dropConstraint('valid_email')
                ->cascade()
                ->toSql()
        );
    }

    public function test_alter_domain_drop_default() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->dropDefault();

        $ast = $builder->toAst();

        self::assertSame('T', $ast->getSubtype());
    }

    public function test_alter_domain_drop_not_null() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->dropNotNull();

        $ast = $builder->toAst();

        self::assertSame('N', $ast->getSubtype());
    }

    public function test_alter_domain_restrict() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->dropConstraint('valid_email')
            ->restrict();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_alter_domain_set_default() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->setDefault(literal('default@example.com'));

        $ast = $builder->toAst();

        self::assertSame('T', $ast->getSubtype());
        self::assertNotNull($ast->getDef());
    }

    public function test_alter_domain_set_not_null() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->setNotNull();

        $ast = $builder->toAst();

        self::assertSame('O', $ast->getSubtype());
    }

    public function test_alter_domain_validate_constraint() : void
    {
        $builder = AlterDomainBuilder::create('email')
            ->validateConstraint('valid_email');

        $ast = $builder->toAst();

        self::assertSame('V', $ast->getSubtype());
        self::assertSame('valid_email', $ast->getName());
    }

    public function test_create_domain_ast_type() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text());

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateDomainStmt::class, $ast);
    }

    public function test_create_domain_sets_name() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text());

        $ast = $builder->toAst();
        $domainname = $ast->getDomainname();

        self::assertCount(1, $domainname);
    }

    public function test_create_domain_sets_type() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text());

        $ast = $builder->toAst();
        $typeName = $ast->getTypeName();

        self::assertNotNull($typeName);
        self::assertCount(2, $typeName->getNames());
    }

    public function test_create_domain_simple_to_sql() : void
    {
        self::assertSame(
            'CREATE DOMAIN email AS pg_catalog.text',
            create()->domain('email')
                ->as(ColumnType::text())
                ->toSql()
        );
    }

    public function test_create_domain_with_check() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text())
            ->check(new OperatorCondition(col('VALUE'), '~', literal('^.+@.+$')));

        $ast = $builder->toAst();
        $constraints = $ast->getConstraints();

        self::assertCount(1, $constraints);

        $constraint = $constraints[0]->getConstraint();
        self::assertNotNull($constraint);
        self::assertSame(ConstrType::CONSTR_CHECK, $constraint->getContype());
    }

    public function test_create_domain_with_collation() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text())
            ->collate('en_US');

        $ast = $builder->toAst();

        self::assertNotNull($ast->getCollClause());
    }

    public function test_create_domain_with_default() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text())
            ->default(literal('default@example.com'));

        $ast = $builder->toAst();
        $constraints = $ast->getConstraints();

        self::assertCount(1, $constraints);

        $constraint = $constraints[0]->getConstraint();
        self::assertNotNull($constraint);
        self::assertSame(ConstrType::CONSTR_DEFAULT, $constraint->getContype());
    }

    public function test_create_domain_with_multiple_constraints() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text())
            ->notNull()
            ->check(new OperatorCondition(col('VALUE'), '~', literal('^.+@.+$')));

        $ast = $builder->toAst();
        $constraints = $ast->getConstraints();

        self::assertCount(2, $constraints);
    }

    public function test_create_domain_with_named_constraint() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text())
            ->constraint('valid_email')
            ->check(new OperatorCondition(col('VALUE'), '~', literal('^.+@.+$')));

        $ast = $builder->toAst();
        $constraints = $ast->getConstraints();

        self::assertCount(1, $constraints);

        $constraint = $constraints[0]->getConstraint();
        self::assertNotNull($constraint);
        self::assertSame('valid_email', $constraint->getConname());
    }

    public function test_create_domain_with_not_null() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text())
            ->notNull();

        $ast = $builder->toAst();
        $constraints = $ast->getConstraints();

        self::assertCount(1, $constraints);

        $constraint = $constraints[0]->getConstraint();
        self::assertNotNull($constraint);
        self::assertSame(ConstrType::CONSTR_NOTNULL, $constraint->getContype());
    }

    public function test_create_domain_with_null() : void
    {
        $builder = CreateDomainBuilder::create('email')
            ->as(column_type_text())
            ->null();

        $ast = $builder->toAst();
        $constraints = $ast->getConstraints();

        self::assertCount(1, $constraints);

        $constraint = $constraints[0]->getConstraint();
        self::assertNotNull($constraint);
        self::assertSame(ConstrType::CONSTR_NULL, $constraint->getContype());
    }

    public function test_create_domain_with_schema() : void
    {
        $builder = CreateDomainBuilder::create('public.email')
            ->as(column_type_text());

        $ast = $builder->toAst();
        $domainname = $ast->getDomainname();

        self::assertCount(2, $domainname);
    }

    public function test_drop_domain_ast_type() : void
    {
        $builder = DropDomainBuilder::create('email');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_DOMAIN, $ast->getRemoveType());
    }

    public function test_drop_domain_cascade() : void
    {
        $builder = DropDomainBuilder::create('email')
            ->cascade();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_domain_if_exists() : void
    {
        $builder = DropDomainBuilder::create('email')
            ->ifExists();

        $ast = $builder->toAst();

        self::assertTrue($ast->getMissingOk());
    }

    public function test_drop_domain_if_exists_cascade_to_sql() : void
    {
        self::assertSame(
            'DROP DOMAIN IF EXISTS email CASCADE',
            drop()->domain('email')
                ->ifExists()
                ->cascade()
                ->toSql()
        );
    }

    public function test_drop_domain_immutability() : void
    {
        $original = DropDomainBuilder::create('email');
        $modified = $original->ifExists();

        self::assertFalse($original->toAst()->getMissingOk());
        self::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_drop_domain_multiple_domains() : void
    {
        $builder = DropDomainBuilder::create('email', 'phone', 'url');

        $ast = $builder->toAst();

        self::assertCount(3, $ast->getObjects());
    }

    public function test_drop_domain_multiple_to_sql() : void
    {
        self::assertSame(
            'DROP DOMAIN email, phone, url',
            drop()->domain('email', 'phone', 'url')
                ->toSql()
        );
    }

    public function test_drop_domain_restrict() : void
    {
        $builder = DropDomainBuilder::create('email')
            ->restrict();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_domain_simple_to_sql() : void
    {
        self::assertSame(
            'DROP DOMAIN email',
            drop()->domain('email')
                ->toSql()
        );
    }
}

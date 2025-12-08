<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\Role;

use Flow\PgQuery\Protobuf\AST\{AlterRoleStmt, CreateRoleStmt, DropRoleStmt, ObjectType, RenameStmt, RoleSpecType, RoleStmtType};
use Flow\PgQuery\QueryBuilder\Schema\Role\{AlterRoleBuilder, CreateRoleBuilder, DropRoleBuilder};
use PHPUnit\Framework\TestCase;

final class RoleBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_role_ast_type() : void
    {
        $builder = AlterRoleBuilder::create('admin')
            ->superuser();

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterRoleStmt::class, $ast);
    }

    public function test_alter_role_rename_ast_type() : void
    {
        $builder = AlterRoleBuilder::create('old_name')
            ->renameTo('new_name');

        $ast = $builder->toAst();

        self::assertInstanceOf(RenameStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_ROLE, $ast->getRenameType());
    }

    public function test_alter_role_rename_sets_names() : void
    {
        $builder = AlterRoleBuilder::create('old_name')
            ->renameTo('new_name');

        $ast = $builder->toAst();

        self::assertSame('old_name', $ast->getSubname());
        self::assertSame('new_name', $ast->getNewname());
    }

    public function test_alter_role_sets_role_spec() : void
    {
        $builder = AlterRoleBuilder::create('admin')
            ->noLogin();

        $ast = $builder->toAst();
        $roleSpec = $ast->getRole();

        self::assertNotNull($roleSpec);
        self::assertSame(RoleSpecType::ROLESPEC_CSTRING, $roleSpec->getRoletype());
        self::assertSame('admin', $roleSpec->getRolename());
    }

    public function test_create_role_ast_type() : void
    {
        $builder = CreateRoleBuilder::create('admin');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateRoleStmt::class, $ast);
        self::assertSame(RoleStmtType::ROLESTMT_ROLE, $ast->getStmtType());
    }

    public function test_create_role_immutability() : void
    {
        $original = CreateRoleBuilder::create('admin');
        $modified = $original->superuser();

        self::assertCount(0, $original->toAst()->getOptions());
        self::assertCount(1, $modified->toAst()->getOptions());
    }

    public function test_create_role_sets_name() : void
    {
        $builder = CreateRoleBuilder::create('admin');

        $ast = $builder->toAst();

        self::assertSame('admin', $ast->getRole());
    }

    public function test_create_role_with_login_option() : void
    {
        $builder = CreateRoleBuilder::create('app_user')
            ->login();

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        self::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        self::assertNotNull($defElem);
        self::assertSame('canlogin', $defElem->getDefname());
    }

    public function test_create_role_with_multiple_options() : void
    {
        $builder = CreateRoleBuilder::create('admin')
            ->superuser()
            ->login()
            ->createDb();

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        self::assertCount(3, $options);
    }

    public function test_create_role_with_password_option() : void
    {
        $builder = CreateRoleBuilder::create('admin')
            ->withPassword('secret');

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        self::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        self::assertNotNull($defElem);
        self::assertSame('password', $defElem->getDefname());
    }

    public function test_create_role_with_superuser_option() : void
    {
        $builder = CreateRoleBuilder::create('admin')
            ->superuser();

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        self::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        self::assertNotNull($defElem);
        self::assertSame('superuser', $defElem->getDefname());
    }

    public function test_drop_role_ast_type() : void
    {
        $builder = DropRoleBuilder::create('admin');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropRoleStmt::class, $ast);
    }

    public function test_drop_role_if_exists_sets_flag() : void
    {
        $builder = DropRoleBuilder::create('admin')
            ->ifExists();

        $ast = $builder->toAst();

        self::assertTrue($ast->getMissingOk());
    }

    public function test_drop_role_immutability() : void
    {
        $original = DropRoleBuilder::create('admin');
        $modified = $original->ifExists();

        self::assertFalse($original->toAst()->getMissingOk());
        self::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_drop_role_multiple_roles() : void
    {
        $builder = DropRoleBuilder::create('role1', 'role2', 'role3');

        $ast = $builder->toAst();

        self::assertCount(3, $ast->getRoles());
    }

    public function test_drop_role_sets_role_names() : void
    {
        $builder = DropRoleBuilder::create('admin');

        $ast = $builder->toAst();
        $roles = $ast->getRoles();

        self::assertCount(1, $roles);

        $roleSpec = $roles[0]->getRoleSpec();
        self::assertNotNull($roleSpec);
        self::assertSame(RoleSpecType::ROLESPEC_CSTRING, $roleSpec->getRoletype());
        self::assertSame('admin', $roleSpec->getRolename());
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Role;

use Flow\PostgreSql\Protobuf\AST\AlterRoleStmt;
use Flow\PostgreSql\Protobuf\AST\CreateRoleStmt;
use Flow\PostgreSql\Protobuf\AST\DropRoleStmt;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\Protobuf\AST\RoleStmtType;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\TablePrivilege;
use Flow\PostgreSql\QueryBuilder\Schema\Role\AlterRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Role\CreateRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Role\DropRoleBuilder;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\grant;
use function Flow\PostgreSql\DSL\grant_role;
use function Flow\PostgreSql\DSL\reassign_owned;
use function Flow\PostgreSql\DSL\reset_role;
use function Flow\PostgreSql\DSL\revoke;
use function Flow\PostgreSql\DSL\revoke_role;
use function Flow\PostgreSql\DSL\set_role;

final class RoleBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_role_ast_type(): void
    {
        $builder = AlterRoleBuilder::create('admin')->superuser();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterRoleStmt::class, $ast);
    }

    public function test_alter_role_rename_ast_type(): void
    {
        $builder = AlterRoleBuilder::create('old_name')->renameTo('new_name');

        $ast = $builder->toAst();

        static::assertInstanceOf(RenameStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_ROLE, $ast->getRenameType());
    }

    public function test_alter_role_rename_sets_names(): void
    {
        $builder = AlterRoleBuilder::create('old_name')->renameTo('new_name');

        $ast = $builder->toAst();

        static::assertSame('old_name', $ast->getSubname());
        static::assertSame('new_name', $ast->getNewname());
    }

    public function test_alter_role_rename_to_sql(): void
    {
        static::assertSame(
            'ALTER ROLE old_name RENAME TO new_name',
            alter()->role('old_name')->renameTo('new_name')->toSql(),
        );
    }

    public function test_alter_role_set_options_to_sql(): void
    {
        static::assertSame('ALTER ROLE admin WITH SUPERUSER', alter()->role('admin')->superuser()->toSql());
    }

    public function test_alter_role_sets_role_spec(): void
    {
        $builder = AlterRoleBuilder::create('admin')->noLogin();

        $ast = $builder->toAst();
        $roleSpec = $ast->getRole();

        static::assertNotNull($roleSpec);
        static::assertSame(RoleSpecType::ROLESPEC_CSTRING, $roleSpec->getRoletype());
        static::assertSame('admin', $roleSpec->getRolename());
    }

    public function test_alter_role_with_multiple_options_to_sql(): void
    {
        static::assertSame(
            'ALTER ROLE admin WITH NOSUPERUSER CREATEDB',
            alter()->role('admin')->noSuperuser()->createDb()->toSql(),
        );
    }

    public function test_create_role_ast_type(): void
    {
        $builder = CreateRoleBuilder::create('admin');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateRoleStmt::class, $ast);
        static::assertSame(RoleStmtType::ROLESTMT_ROLE, $ast->getStmtType());
    }

    public function test_create_role_immutability(): void
    {
        $original = CreateRoleBuilder::create('admin');
        $modified = $original->superuser();

        static::assertCount(0, $original->toAst()->getOptions());
        static::assertCount(1, $modified->toAst()->getOptions());
    }

    public function test_create_role_sets_name(): void
    {
        $builder = CreateRoleBuilder::create('admin');

        $ast = $builder->toAst();

        static::assertSame('admin', $ast->getRole());
    }

    public function test_create_role_simple_to_sql(): void
    {
        static::assertSame('CREATE ROLE admin', create()->role('admin')->toSql());
    }

    public function test_create_role_with_login_option(): void
    {
        $builder = CreateRoleBuilder::create('app_user')->login();

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        static::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('canlogin', $defElem->getDefname());
    }

    public function test_create_role_with_login_to_sql(): void
    {
        static::assertSame('CREATE ROLE app_user WITH LOGIN', create()->role('app_user')->login()->toSql());
    }

    public function test_create_role_with_multiple_options(): void
    {
        $builder = CreateRoleBuilder::create('admin')->superuser()->login()->createDb();

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        static::assertCount(3, $options);
    }

    public function test_create_role_with_options_to_sql(): void
    {
        static::assertSame(
            'CREATE ROLE admin WITH SUPERUSER LOGIN',
            create()->role('admin')->superuser()->login()->toSql(),
        );
    }

    public function test_create_role_with_password_option(): void
    {
        $builder = CreateRoleBuilder::create('admin')->withPassword('secret');

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        static::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('password', $defElem->getDefname());
    }

    public function test_create_role_with_superuser_option(): void
    {
        $builder = CreateRoleBuilder::create('admin')->superuser();

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        static::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('superuser', $defElem->getDefname());
    }

    public function test_create_user_with_password_to_sql(): void
    {
        static::assertSame(
            "CREATE ROLE app_user WITH LOGIN PASSWORD 'secret'",
            create()->role('app_user')->login()->withPassword('secret')->toSql(),
        );
    }

    public function test_drop_owned_by_cascade_to_sql(): void
    {
        static::assertSame('DROP OWNED BY role1 CASCADE', drop()->owned('role1')->cascade()->toSql());
    }

    public function test_drop_owned_by_to_sql(): void
    {
        static::assertSame('DROP OWNED BY role1', drop()->owned('role1')->toSql());
    }

    public function test_drop_role_ast_type(): void
    {
        $builder = DropRoleBuilder::create('admin');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropRoleStmt::class, $ast);
    }

    public function test_drop_role_if_exists_sets_flag(): void
    {
        $builder = DropRoleBuilder::create('admin')->ifExists();

        $ast = $builder->toAst();

        static::assertTrue($ast->getMissingOk());
    }

    public function test_drop_role_if_exists_to_sql(): void
    {
        static::assertSame('DROP ROLE IF EXISTS admin', drop()->role('admin')->ifExists()->toSql());
    }

    public function test_drop_role_immutability(): void
    {
        $original = DropRoleBuilder::create('admin');
        $modified = $original->ifExists();

        static::assertFalse($original->toAst()->getMissingOk());
        static::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_drop_role_multiple_roles(): void
    {
        $builder = DropRoleBuilder::create('role1', 'role2', 'role3');

        $ast = $builder->toAst();

        static::assertCount(3, $ast->getRoles());
    }

    public function test_drop_role_multiple_to_sql(): void
    {
        static::assertSame('DROP ROLE role1, role2', drop()->role('role1', 'role2')->toSql());
    }

    public function test_drop_role_sets_role_names(): void
    {
        $builder = DropRoleBuilder::create('admin');

        $ast = $builder->toAst();
        $roles = $ast->getRoles();

        static::assertCount(1, $roles);

        $roleSpec = $roles[0]->getRoleSpec();
        static::assertNotNull($roleSpec);
        static::assertSame(RoleSpecType::ROLESPEC_CSTRING, $roleSpec->getRoletype());
        static::assertSame('admin', $roleSpec->getRolename());
    }

    public function test_drop_role_simple_to_sql(): void
    {
        static::assertSame('DROP ROLE admin', drop()->role('admin')->toSql());
    }

    public function test_grant_all_on_table_to_sql(): void
    {
        static::assertSame(
            'GRANT ALL ON users TO admin',
            grant(TablePrivilege::ALL)->onTable('users')->to('admin')->toSql(),
        );
    }

    public function test_grant_on_all_tables_in_schema_to_sql(): void
    {
        static::assertSame(
            'GRANT select ON ALL TABLES IN SCHEMA public TO admin',
            grant(TablePrivilege::SELECT)->onAllTablesInSchema('public')->to('admin')->toSql(),
        );
    }

    public function test_grant_role_to_role_to_sql(): void
    {
        static::assertSame('GRANT admin TO user1', grant_role('admin')->to('user1')->toSql());
    }

    public function test_grant_role_with_admin_option_to_sql(): void
    {
        static::assertSame(
            'GRANT admin TO user1 WITH ADMIN OPTION',
            grant_role('admin')->to('user1')->withAdminOption()->toSql(),
        );
    }

    public function test_grant_select_on_table_to_sql(): void
    {
        static::assertSame(
            'GRANT select ON users TO app_user',
            grant(TablePrivilege::SELECT)->onTable('users')->to('app_user')->toSql(),
        );
    }

    public function test_grant_with_grant_option_to_sql(): void
    {
        static::assertSame(
            'GRANT select ON users TO app_user WITH GRANT OPTION',
            grant(TablePrivilege::SELECT)->onTable('users')->to('app_user')->withGrantOption()->toSql(),
        );
    }

    public function test_reassign_owned_by_to_sql(): void
    {
        static::assertSame(
            'REASSIGN OWNED BY old_role TO new_role',
            reassign_owned('old_role')->to('new_role')->toSql(),
        );
    }

    public function test_reset_role_to_sql(): void
    {
        static::assertSame('RESET role', reset_role()->toSql());
    }

    public function test_revoke_role_cascade_to_sql(): void
    {
        static::assertSame('REVOKE admin FROM user1 CASCADE', revoke_role('admin')->from('user1')->cascade()->toSql());
    }

    public function test_revoke_role_from_role_to_sql(): void
    {
        static::assertSame('REVOKE admin FROM user1', revoke_role('admin')->from('user1')->toSql());
    }

    public function test_revoke_select_on_table_to_sql(): void
    {
        static::assertSame(
            'REVOKE select ON users FROM app_user',
            revoke(TablePrivilege::SELECT)->onTable('users')->from('app_user')->toSql(),
        );
    }

    public function test_revoke_with_cascade_to_sql(): void
    {
        static::assertSame(
            'REVOKE select ON users FROM app_user CASCADE',
            revoke(TablePrivilege::SELECT)->onTable('users')->from('app_user')->cascade()->toSql(),
        );
    }

    public function test_set_role_to_sql(): void
    {
        static::assertSame('SET role TO admin', set_role('admin')->toSql());
    }
}

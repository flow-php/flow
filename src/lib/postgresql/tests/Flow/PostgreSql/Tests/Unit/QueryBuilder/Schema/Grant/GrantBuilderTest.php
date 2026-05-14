<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\GrantRoleStmt;
use Flow\PostgreSql\Protobuf\AST\GrantStmt;
use Flow\PostgreSql\Protobuf\AST\GrantTargetType;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\GrantBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\GrantRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\RevokeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\RevokeRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Grant\TablePrivilege;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;

final class GrantBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_grant_all_privileges_has_empty_privileges_array(): void
    {
        $builder = GrantBuilder::create(TablePrivilege::ALL)->onTable('users')->to('app_user');

        $ast = $builder->toAst();

        static::assertCount(0, $ast->getPrivileges());
    }

    public function test_grant_ast_type(): void
    {
        $builder = GrantBuilder::create(TablePrivilege::SELECT)->onTable('users')->to('app_user');

        $ast = $builder->toAst();

        static::assertInstanceOf(GrantStmt::class, $ast);
        static::assertTrue($ast->getIsGrant());
    }

    public function test_grant_immutability(): void
    {
        $original = GrantBuilder::create(TablePrivilege::SELECT)->onTable('users')->to('app_user');
        $modified = $original->withGrantOption();

        static::assertFalse($original->toAst()->getGrantOption());
        static::assertTrue($modified->toAst()->getGrantOption());
    }

    public function test_grant_on_all_tables_in_schema(): void
    {
        $builder = GrantBuilder::create(TablePrivilege::SELECT)->onAllTablesInSchema('public')->to('admin');

        $ast = $builder->toAst();

        static::assertSame(GrantTargetType::ACL_TARGET_ALL_IN_SCHEMA, $ast->getTargtype());
    }

    public function test_grant_privileges_on_table(): void
    {
        $builder = GrantBuilder::create(TablePrivilege::SELECT, TablePrivilege::INSERT)->onTable('users')->to(
            'app_user',
        );

        $ast = $builder->toAst();

        static::assertSame(GrantTargetType::ACL_TARGET_OBJECT, $ast->getTargtype());
        static::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        static::assertCount(2, $ast->getPrivileges());
    }

    public function test_grant_role_ast_type(): void
    {
        $builder = GrantRoleBuilder::create('admin')->to('user1');

        $ast = $builder->toAst();

        static::assertInstanceOf(GrantRoleStmt::class, $ast);
        static::assertTrue($ast->getIsGrant());
    }

    public function test_grant_role_sets_granted_roles(): void
    {
        $builder = GrantRoleBuilder::create('admin', 'developer')->to('user1');

        $ast = $builder->toAst();

        static::assertCount(2, $ast->getGrantedRoles());
    }

    public function test_grant_role_with_admin_option(): void
    {
        $builder = GrantRoleBuilder::create('admin')->to('user1')->withAdminOption();

        $ast = $builder->toAst();
        $opts = $ast->getOpt();

        static::assertCount(1, $opts);

        $defElem = $opts[0]->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('admin', $defElem->getDefname());

        $arg = $defElem->getArg();
        static::assertNotNull($arg);
        $boolean = type_instance_of(Boolean::class)->assert($arg->getBoolean());
        static::assertTrue($boolean->getBoolval());
    }

    public function test_grant_to_public(): void
    {
        $builder = GrantBuilder::create(TablePrivilege::SELECT)->onTable('users')->toPublic();

        $ast = $builder->toAst();
        $grantees = $ast->getGrantees();

        static::assertCount(1, $grantees);

        $roleSpec = $grantees[0]->getRoleSpec();
        static::assertNotNull($roleSpec);
        static::assertSame(RoleSpecType::ROLESPEC_PUBLIC, $roleSpec->getRoletype());
    }

    public function test_grant_with_grant_option(): void
    {
        $builder = GrantBuilder::create(TablePrivilege::SELECT)->onTable('users')->to('app_user')->withGrantOption();

        $ast = $builder->toAst();

        static::assertTrue($ast->getGrantOption());
    }

    public function test_revoke_ast_type(): void
    {
        $builder = RevokeBuilder::create(TablePrivilege::SELECT)->onTable('users')->from('app_user');

        $ast = $builder->toAst();

        static::assertInstanceOf(GrantStmt::class, $ast);
        static::assertFalse($ast->getIsGrant());
    }

    public function test_revoke_cascade_sets_behavior(): void
    {
        $builder = RevokeBuilder::create(TablePrivilege::SELECT)->onTable('users')->from('app_user')->cascade();

        $ast = $builder->toAst();

        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_revoke_restrict_sets_behavior(): void
    {
        $builder = RevokeBuilder::create(TablePrivilege::SELECT)->onTable('users')->from('app_user')->restrict();

        $ast = $builder->toAst();

        static::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_revoke_role_ast_type(): void
    {
        $builder = RevokeRoleBuilder::create('admin')->from('user1');

        $ast = $builder->toAst();

        static::assertInstanceOf(GrantRoleStmt::class, $ast);
        static::assertFalse($ast->getIsGrant());
    }

    public function test_revoke_role_cascade_sets_behavior(): void
    {
        $builder = RevokeRoleBuilder::create('admin')->from('user1')->cascade();

        $ast = $builder->toAst();

        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }
}

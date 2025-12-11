<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Ownership;

use Flow\PostgreSql\Protobuf\AST\{DropBehavior, DropOwnedStmt, ReassignOwnedStmt, RoleSpecType};
use Flow\PostgreSql\QueryBuilder\Schema\Ownership\{DropOwnedBuilder, ReassignOwnedBuilder};
use PHPUnit\Framework\TestCase;

final class OwnershipBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_drop_owned_ast_type() : void
    {
        $builder = DropOwnedBuilder::create('role1');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropOwnedStmt::class, $ast);
    }

    public function test_drop_owned_cascade_sets_behavior() : void
    {
        $builder = DropOwnedBuilder::create('role1')
            ->cascade();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_owned_immutability() : void
    {
        $original = DropOwnedBuilder::create('role1');
        $modified = $original->cascade();

        self::assertSame(DropBehavior::DROP_RESTRICT, $original->toAst()->getBehavior());
        self::assertSame(DropBehavior::DROP_CASCADE, $modified->toAst()->getBehavior());
    }

    public function test_drop_owned_restrict_sets_behavior() : void
    {
        $builder = DropOwnedBuilder::create('role1')
            ->restrict();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_owned_sets_roles() : void
    {
        $builder = DropOwnedBuilder::create('role1', 'role2');

        $ast = $builder->toAst();
        $roles = $ast->getRoles();

        self::assertCount(2, $roles);
    }

    public function test_reassign_owned_ast_type() : void
    {
        $builder = ReassignOwnedBuilder::create('old_role')
            ->to('new_role');

        $ast = $builder->toAst();

        self::assertInstanceOf(ReassignOwnedStmt::class, $ast);
    }

    public function test_reassign_owned_sets_new_role() : void
    {
        $builder = ReassignOwnedBuilder::create('old_role')
            ->to('new_owner');

        $ast = $builder->toAst();
        $newRole = $ast->getNewrole();

        self::assertNotNull($newRole);
        self::assertSame(RoleSpecType::ROLESPEC_CSTRING, $newRole->getRoletype());
        self::assertSame('new_owner', $newRole->getRolename());
    }

    public function test_reassign_owned_sets_roles() : void
    {
        $builder = ReassignOwnedBuilder::create('role1', 'role2')
            ->to('new_role');

        $ast = $builder->toAst();
        $roles = $ast->getRoles();

        self::assertCount(2, $roles);

        $roleSpec = $roles[0]->getRoleSpec();
        self::assertNotNull($roleSpec);
        self::assertSame(RoleSpecType::ROLESPEC_CSTRING, $roleSpec->getRoletype());
        self::assertSame('role1', $roleSpec->getRolename());
    }
}

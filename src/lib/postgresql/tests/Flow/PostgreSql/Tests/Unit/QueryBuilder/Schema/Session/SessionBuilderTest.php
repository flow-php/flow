<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Session;

use Flow\PostgreSql\Protobuf\AST\{VariableSetKind, VariableSetStmt};
use Flow\PostgreSql\QueryBuilder\Schema\Session\{ResetRoleBuilder, SetRoleBuilder};
use PHPUnit\Framework\TestCase;

final class SessionBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_reset_role_ast_type() : void
    {
        $builder = ResetRoleBuilder::create();

        $ast = $builder->toAst();

        self::assertInstanceOf(VariableSetStmt::class, $ast);
        self::assertSame(VariableSetKind::VAR_RESET, $ast->getKind());
    }

    public function test_reset_role_has_no_args() : void
    {
        $builder = ResetRoleBuilder::create();

        $ast = $builder->toAst();

        self::assertCount(0, $ast->getArgs());
    }

    public function test_reset_role_sets_name_to_role() : void
    {
        $builder = ResetRoleBuilder::create();

        $ast = $builder->toAst();

        self::assertSame('role', $ast->getName());
    }

    public function test_set_role_ast_type() : void
    {
        $builder = SetRoleBuilder::create('admin');

        $ast = $builder->toAst();

        self::assertInstanceOf(VariableSetStmt::class, $ast);
        self::assertSame(VariableSetKind::VAR_SET_VALUE, $ast->getKind());
    }

    public function test_set_role_sets_name_to_role() : void
    {
        $builder = SetRoleBuilder::create('admin');

        $ast = $builder->toAst();

        self::assertSame('role', $ast->getName());
    }

    public function test_set_role_sets_role_in_args() : void
    {
        $builder = SetRoleBuilder::create('admin');

        $ast = $builder->toAst();
        $args = $ast->getArgs();

        self::assertCount(1, $args);

        $aConst = $args[0]->getAConst();
        self::assertNotNull($aConst);
        self::assertSame('admin', $aConst->getSval()->getSval());
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Session;

use Flow\PostgreSql\Protobuf\AST\VariableSetKind;
use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Session\ResetRoleBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Session\SetRoleBuilder;
use PHPUnit\Framework\TestCase;

final class SessionBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_reset_role_ast_type(): void
    {
        $builder = ResetRoleBuilder::create();

        $ast = $builder->toAst();

        static::assertInstanceOf(VariableSetStmt::class, $ast);
        static::assertSame(VariableSetKind::VAR_RESET, $ast->getKind());
    }

    public function test_reset_role_has_no_args(): void
    {
        $builder = ResetRoleBuilder::create();

        $ast = $builder->toAst();

        static::assertCount(0, $ast->getArgs());
    }

    public function test_reset_role_sets_name_to_role(): void
    {
        $builder = ResetRoleBuilder::create();

        $ast = $builder->toAst();

        static::assertSame('role', $ast->getName());
    }

    public function test_set_role_ast_type(): void
    {
        $builder = SetRoleBuilder::create('admin');

        $ast = $builder->toAst();

        static::assertInstanceOf(VariableSetStmt::class, $ast);
        static::assertSame(VariableSetKind::VAR_SET_VALUE, $ast->getKind());
    }

    public function test_set_role_sets_name_to_role(): void
    {
        $builder = SetRoleBuilder::create('admin');

        $ast = $builder->toAst();

        static::assertSame('role', $ast->getName());
    }

    public function test_set_role_sets_role_in_args(): void
    {
        $builder = SetRoleBuilder::create('admin');

        $ast = $builder->toAst();
        $args = $ast->getArgs();

        static::assertCount(1, $args);

        $aConst = $args[0]->getAConst();
        static::assertNotNull($aConst);
        $sval = $aConst->getSval();
        static::assertNotNull($sval);
        static::assertSame('admin', $sval->getSval());
    }
}

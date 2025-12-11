<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\VacuumStmt;
use Flow\PostgreSql\QueryBuilder\Utility\AnalyzeBuilder;
use PHPUnit\Framework\TestCase;

final class AnalyzeBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_analyze_multiple_tables() : void
    {
        $builder = AnalyzeBuilder::create()->tables('users', 'orders');

        $ast = $builder->toAst();

        self::assertCount(2, $ast->getRels());
        $rel0 = $ast->getRels()[0]->getVacuumRelation();
        $rel1 = $ast->getRels()[1]->getVacuumRelation();
        self::assertNotNull($rel0);
        self::assertNotNull($rel1);
        self::assertSame('users', $rel0->getRelation()?->getRelname());
        self::assertSame('orders', $rel1->getRelation()?->getRelname());
    }

    public function test_analyze_skip_locked_option() : void
    {
        $builder = AnalyzeBuilder::create()->skipLocked();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('skip_locked', $optionNames);
    }

    public function test_analyze_table_with_schema() : void
    {
        $builder = AnalyzeBuilder::create()->table('public.users');

        $ast = $builder->toAst();

        self::assertCount(1, $ast->getRels());
        $rel = $ast->getRels()[0]->getVacuumRelation();
        self::assertNotNull($rel);
        $relation = $rel->getRelation();
        self::assertNotNull($relation);
        self::assertSame('public', $relation->getSchemaname());
        self::assertSame('users', $relation->getRelname());
    }

    public function test_analyze_verbose_option() : void
    {
        $builder = AnalyzeBuilder::create()->verbose();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('verbose', $optionNames);
    }

    public function test_analyze_with_table() : void
    {
        $builder = AnalyzeBuilder::create()->table('users');

        $ast = $builder->toAst();

        self::assertCount(1, $ast->getRels());
        $rel = $ast->getRels()[0]->getVacuumRelation();
        self::assertNotNull($rel);
        self::assertSame('users', $rel->getRelation()?->getRelname());
    }

    public function test_analyze_with_table_and_columns() : void
    {
        $builder = AnalyzeBuilder::create()->table('users', 'email', 'name');

        $ast = $builder->toAst();

        self::assertCount(1, $ast->getRels());
        $rel = $ast->getRels()[0]->getVacuumRelation();
        self::assertNotNull($rel);
        self::assertSame('users', $rel->getRelation()?->getRelname());
        self::assertCount(2, $rel->getVaCols());
    }

    public function test_basic_analyze() : void
    {
        $builder = AnalyzeBuilder::create();

        $ast = $builder->toAst();

        self::assertInstanceOf(VacuumStmt::class, $ast);
        self::assertFalse($ast->getIsVacuumcmd());
        self::assertCount(0, $ast->getRels());
    }

    public function test_immutability() : void
    {
        $original = AnalyzeBuilder::create();
        $modified = $original->verbose();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        self::assertCount(0, $originalAst->getOptions());
        self::assertCount(1, $modifiedAst->getOptions());
    }
}

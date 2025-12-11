<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\VacuumStmt;
use Flow\PostgreSql\QueryBuilder\Utility\{IndexCleanup, VacuumBuilder};
use PHPUnit\Framework\TestCase;

final class VacuumBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_basic_vacuum() : void
    {
        $builder = VacuumBuilder::create();

        $ast = $builder->toAst();

        self::assertInstanceOf(VacuumStmt::class, $ast);
        self::assertTrue($ast->getIsVacuumcmd());
        self::assertCount(0, $ast->getRels());
    }

    public function test_immutability() : void
    {
        $original = VacuumBuilder::create();
        $modified = $original->full();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        self::assertCount(0, $originalAst->getOptions());
        self::assertCount(1, $modifiedAst->getOptions());
    }

    public function test_vacuum_analyze_option() : void
    {
        $builder = VacuumBuilder::create()->analyze();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('analyze', $optionNames);
    }

    public function test_vacuum_disable_page_skipping_option() : void
    {
        $builder = VacuumBuilder::create()->disablePageSkipping();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('disable_page_skipping', $optionNames);
    }

    public function test_vacuum_freeze_option() : void
    {
        $builder = VacuumBuilder::create()->freeze();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('freeze', $optionNames);
    }

    public function test_vacuum_full_option() : void
    {
        $builder = VacuumBuilder::create()->full();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('full', $optionNames);
    }

    public function test_vacuum_index_cleanup_option() : void
    {
        $builder = VacuumBuilder::create()->indexCleanup(IndexCleanup::ON);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $indexCleanupFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem?->getDefname() === 'index_cleanup') {
                $indexCleanupFound = true;
                self::assertSame('on', $defElem->getArg()?->getString()?->getSval());
            }
        }
        self::assertTrue($indexCleanupFound);
    }

    public function test_vacuum_multiple_tables() : void
    {
        $builder = VacuumBuilder::create()->tables('users', 'orders', 'products');

        $ast = $builder->toAst();

        self::assertCount(3, $ast->getRels());
        $rel0 = $ast->getRels()[0]->getVacuumRelation();
        $rel1 = $ast->getRels()[1]->getVacuumRelation();
        $rel2 = $ast->getRels()[2]->getVacuumRelation();
        self::assertNotNull($rel0);
        self::assertNotNull($rel1);
        self::assertNotNull($rel2);
        self::assertSame('users', $rel0->getRelation()?->getRelname());
        self::assertSame('orders', $rel1->getRelation()?->getRelname());
        self::assertSame('products', $rel2->getRelation()?->getRelname());
    }

    public function test_vacuum_parallel_option() : void
    {
        $builder = VacuumBuilder::create()->parallel(4);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $parallelFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem?->getDefname() === 'parallel') {
                $parallelFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                self::assertSame(4, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        self::assertTrue($parallelFound);
    }

    public function test_vacuum_skip_locked_option() : void
    {
        $builder = VacuumBuilder::create()->skipLocked();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('skip_locked', $optionNames);
    }

    public function test_vacuum_table_with_schema() : void
    {
        $builder = VacuumBuilder::create()->table('public.users');

        $ast = $builder->toAst();

        self::assertCount(1, $ast->getRels());
        $rel = $ast->getRels()[0]->getVacuumRelation();
        self::assertNotNull($rel);
        self::assertSame('public', $rel->getRelation()?->getSchemaname());
        self::assertSame('users', $rel->getRelation()?->getRelname());
    }

    public function test_vacuum_verbose_option() : void
    {
        $builder = VacuumBuilder::create()->verbose();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('verbose', $optionNames);
    }

    public function test_vacuum_with_table() : void
    {
        $builder = VacuumBuilder::create()->table('users');

        $ast = $builder->toAst();

        self::assertCount(1, $ast->getRels());
        $rel = $ast->getRels()[0]->getVacuumRelation();
        self::assertNotNull($rel);
        self::assertSame('users', $rel->getRelation()?->getRelname());
    }

    public function test_vacuum_with_table_and_columns() : void
    {
        $builder = VacuumBuilder::create()->table('users', 'email', 'name');

        $ast = $builder->toAst();

        self::assertCount(1, $ast->getRels());
        $rel = $ast->getRels()[0]->getVacuumRelation();
        self::assertNotNull($rel);
        self::assertSame('users', $rel->getRelation()?->getRelname());
        self::assertCount(2, $rel->getVaCols());
    }
}

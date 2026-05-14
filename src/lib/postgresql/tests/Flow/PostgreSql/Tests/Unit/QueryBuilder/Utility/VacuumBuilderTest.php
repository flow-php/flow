<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\VacuumStmt;
use Flow\PostgreSql\QueryBuilder\Utility\IndexCleanup;
use Flow\PostgreSql\QueryBuilder\Utility\VacuumBuilder;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;

final class VacuumBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_basic_vacuum(): void
    {
        $builder = VacuumBuilder::create();

        $ast = $builder->toAst();

        static::assertInstanceOf(VacuumStmt::class, $ast);
        static::assertTrue($ast->getIsVacuumcmd());
        static::assertCount(0, $ast->getRels());
    }

    public function test_immutability(): void
    {
        $original = VacuumBuilder::create();
        $modified = $original->full();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        static::assertCount(0, $originalAst->getOptions());
        static::assertCount(1, $modifiedAst->getOptions());
    }

    public function test_vacuum_analyze_option(): void
    {
        $builder = VacuumBuilder::create()->analyze();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        static::assertContains('analyze', $optionNames);
    }

    public function test_vacuum_disable_page_skipping_option(): void
    {
        $builder = VacuumBuilder::create()->disablePageSkipping();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        static::assertContains('disable_page_skipping', $optionNames);
    }

    public function test_vacuum_freeze_option(): void
    {
        $builder = VacuumBuilder::create()->freeze();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        static::assertContains('freeze', $optionNames);
    }

    public function test_vacuum_full_option(): void
    {
        $builder = VacuumBuilder::create()->full();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        static::assertContains('full', $optionNames);
    }

    public function test_vacuum_index_cleanup_option(): void
    {
        $builder = VacuumBuilder::create()->indexCleanup(IndexCleanup::ON);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $indexCleanupFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem !== null && $defElem->getDefname() === 'index_cleanup') {
                $indexCleanupFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $string = $arg->getString();
                static::assertNotNull($string);
                static::assertSame('on', $string->getSval());
            }
        }
        static::assertTrue($indexCleanupFound);
    }

    public function test_vacuum_multiple_tables(): void
    {
        $builder = VacuumBuilder::create()->tables('users', 'orders', 'products');

        $ast = $builder->toAst();

        static::assertCount(3, $ast->getRels());
        $rel0 = $ast->getRels()[0]->getVacuumRelation();
        $rel1 = $ast->getRels()[1]->getVacuumRelation();
        $rel2 = $ast->getRels()[2]->getVacuumRelation();
        static::assertNotNull($rel0);
        static::assertNotNull($rel1);
        static::assertNotNull($rel2);
        static::assertSame('users', $rel0->getRelation()?->getRelname());
        static::assertSame('orders', $rel1->getRelation()?->getRelname());
        static::assertSame('products', $rel2->getRelation()?->getRelname());
    }

    public function test_vacuum_parallel_option(): void
    {
        $builder = VacuumBuilder::create()->parallel(4);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $parallelFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem !== null && $defElem->getDefname() === 'parallel') {
                $parallelFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(4, $integer->getIval());
            }
        }
        static::assertTrue($parallelFound);
    }

    public function test_vacuum_skip_locked_option(): void
    {
        $builder = VacuumBuilder::create()->skipLocked();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        static::assertContains('skip_locked', $optionNames);
    }

    public function test_vacuum_table_with_schema(): void
    {
        $builder = VacuumBuilder::create()->table('public.users');

        $ast = $builder->toAst();

        static::assertCount(1, $ast->getRels());
        $rel = $ast->getRels()[0]->getVacuumRelation();
        static::assertNotNull($rel);
        static::assertSame('public', $rel->getRelation()?->getSchemaname());
        static::assertSame('users', $rel->getRelation()?->getRelname());
    }

    public function test_vacuum_verbose_option(): void
    {
        $builder = VacuumBuilder::create()->verbose();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        static::assertContains('verbose', $optionNames);
    }

    public function test_vacuum_with_table(): void
    {
        $builder = VacuumBuilder::create()->table('users');

        $ast = $builder->toAst();

        static::assertCount(1, $ast->getRels());
        $rel = $ast->getRels()[0]->getVacuumRelation();
        static::assertNotNull($rel);
        static::assertSame('users', $rel->getRelation()?->getRelname());
    }

    public function test_vacuum_with_table_and_columns(): void
    {
        $builder = VacuumBuilder::create()->table('users', 'email', 'name');

        $ast = $builder->toAst();

        static::assertCount(1, $ast->getRels());
        $rel = $ast->getRels()[0]->getVacuumRelation();
        static::assertNotNull($rel);
        static::assertSame('users', $rel->getRelation()?->getRelname());
        static::assertCount(2, $rel->getVaCols());
    }
}

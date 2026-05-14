<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\ExplainStmt;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\Types\DSL\type_instance_of;

final class ExplainBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_basic_explain(): void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query);

        $ast = $builder->toAst();

        static::assertInstanceOf(ExplainStmt::class, $ast);
        static::assertNotNull($ast->getQuery());
        static::assertCount(0, $ast->getOptions());
    }

    public function test_explain_analyze_option(): void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->analyze();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        static::assertContains('analyze', $optionNames);
    }

    public function test_explain_buffers_option(): void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->buffers(true);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $buffersFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem !== null && $defElem->getDefname() === 'buffers') {
                $buffersFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(1, $integer->getIval());
            }
        }
        static::assertTrue($buffersFound);
    }

    public function test_explain_costs_option(): void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->costs(true);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $costsFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem !== null && $defElem->getDefname() === 'costs') {
                $costsFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(1, $integer->getIval());
            }
        }
        static::assertTrue($costsFound);
    }

    public function test_explain_format_json(): void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->format(ExplainFormat::JSON);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $formatFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem !== null && $defElem->getDefname() === 'format') {
                $formatFound = true;
                static::assertSame('json', $defElem->getArg()?->getString()?->getSval());
            }
        }
        static::assertTrue($formatFound);
    }

    public function test_explain_format_yaml(): void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->format(ExplainFormat::YAML);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $formatFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem !== null && $defElem->getDefname() === 'format') {
                $formatFound = true;
                static::assertSame('yaml', $defElem->getArg()?->getString()?->getSval());
            }
        }
        static::assertTrue($formatFound);
    }

    public function test_explain_timing_option(): void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->timing(true);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $timingFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem !== null && $defElem->getDefname() === 'timing') {
                $timingFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(1, $integer->getIval());
            }
        }
        static::assertTrue($timingFound);
    }

    public function test_explain_verbose_option(): void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->verbose();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        static::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        static::assertContains('verbose', $optionNames);
    }

    public function test_immutability(): void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $original = ExplainBuilder::create($query);
        $modified = $original->analyze();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        static::assertCount(0, $originalAst->getOptions());
        static::assertCount(1, $modifiedAst->getOptions());
    }
}

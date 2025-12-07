<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Utility;

use function Flow\PgQuery\DSL\{star, table};

use Flow\PgQuery\Protobuf\AST\ExplainStmt;
use Flow\PgQuery\QueryBuilder\Select\SelectBuilder;
use Flow\PgQuery\QueryBuilder\Utility\{ExplainBuilder, ExplainFormat};
use PHPUnit\Framework\TestCase;

final class ExplainBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_basic_explain() : void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query);

        $ast = $builder->toAst();

        self::assertInstanceOf(ExplainStmt::class, $ast);
        self::assertNotNull($ast->getQuery());
        self::assertCount(0, $ast->getOptions());
    }

    public function test_explain_analyze_option() : void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->analyze();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('analyze', $optionNames);
    }

    public function test_explain_buffers_option() : void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->buffers(true);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $buffersFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem?->getDefname() === 'buffers') {
                $buffersFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                self::assertSame(1, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        self::assertTrue($buffersFound);
    }

    public function test_explain_costs_option() : void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->costs(true);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $costsFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem?->getDefname() === 'costs') {
                $costsFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                self::assertSame(1, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        self::assertTrue($costsFound);
    }

    public function test_explain_format_json() : void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->format(ExplainFormat::JSON);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $formatFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem?->getDefname() === 'format') {
                $formatFound = true;
                self::assertSame('json', $defElem->getArg()?->getString()?->getSval());
            }
        }
        self::assertTrue($formatFound);
    }

    public function test_explain_format_yaml() : void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->format(ExplainFormat::YAML);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $formatFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem?->getDefname() === 'format') {
                $formatFound = true;
                self::assertSame('yaml', $defElem->getArg()?->getString()?->getSval());
            }
        }
        self::assertTrue($formatFound);
    }

    public function test_explain_timing_option() : void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->timing(true);

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $timingFound = false;

        foreach ($options as $opt) {
            $defElem = $opt->getDefElem();

            if ($defElem?->getDefname() === 'timing') {
                $timingFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                self::assertSame(1, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        self::assertTrue($timingFound);
    }

    public function test_explain_verbose_option() : void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $builder = ExplainBuilder::create($query)->verbose();

        $ast = $builder->toAst();

        $options = $ast->getOptions();
        self::assertNotEmpty($options);
        $optionNames = [];

        foreach ($options as $opt) {
            $optionNames[] = $opt->getDefElem()?->getDefname();
        }
        self::assertContains('verbose', $optionNames);
    }

    public function test_immutability() : void
    {
        $query = SelectBuilder::create()->select(star())->from(table('users'));
        $original = ExplainBuilder::create($query);
        $modified = $original->analyze();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        self::assertCount(0, $originalAst->getOptions());
        self::assertCount(1, $modifiedAst->getOptions());
    }
}

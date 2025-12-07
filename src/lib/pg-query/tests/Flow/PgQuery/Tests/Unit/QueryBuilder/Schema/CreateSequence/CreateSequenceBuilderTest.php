<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\CreateSequence;

use Flow\PgQuery\Protobuf\AST\CreateSeqStmt;
use Flow\PgQuery\QueryBuilder\Schema\CreateSequence\CreateSequenceBuilder;
use PHPUnit\Framework\TestCase;

final class CreateSequenceBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_create_sequence_if_not_exists() : void
    {
        $builder = CreateSequenceBuilder::createIfNotExists()
            ->sequence('user_id_seq');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertTrue($ast->getIfNotExists());
    }

    public function test_create_sequence_no_max_value() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->noMaxValue();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'maxvalue') {
                $optionFound = true;
                self::assertFalse($defElem->hasArg());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_no_min_value() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->noMinValue();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'minvalue') {
                $optionFound = true;
                self::assertFalse($defElem->hasArg());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_owned_by() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->ownedBy('users', 'id');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'owned_by') {
                $optionFound = true;
                self::assertTrue($defElem->hasArg());
                $list = $defElem->getArg()?->getList();
                self::assertNotNull($list);
                self::assertCount(2, $list->getItems());
                self::assertSame('users', $list->getItems()[0]->getString()?->getSval());
                self::assertSame('id', $list->getItems()[1]->getString()?->getSval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_owned_by_none() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->ownedByNone();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'owned_by') {
                $optionFound = true;
                self::assertTrue($defElem->hasArg());
                $list = $defElem->getArg()?->getList();
                self::assertNotNull($list);
                self::assertCount(1, $list->getItems());
                self::assertSame('none', $list->getItems()[0]->getString()?->getSval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_owned_by_with_schema() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->ownedBy('public.users', 'id');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'owned_by') {
                $optionFound = true;
                self::assertTrue($defElem->hasArg());
                $list = $defElem->getArg()?->getList();
                self::assertNotNull($list);
                self::assertCount(3, $list->getItems());
                self::assertSame('public', $list->getItems()[0]->getString()?->getSval());
                self::assertSame('users', $list->getItems()[1]->getString()?->getSval());
                self::assertSame('id', $list->getItems()[2]->getString()?->getSval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_with_all_options() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->asType('bigint')
            ->startWith(1)
            ->incrementBy(1)
            ->minValue(1)
            ->maxValue(9223372036854775807)
            ->cache(1)
            ->noCycle();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertCount(7, $ast->getOptions());
    }

    public function test_create_sequence_with_as_type() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->asType('bigint');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'as') {
                $optionFound = true;
                self::assertTrue($defElem->hasArg());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_with_cache() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->cache(20);

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'cache') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                self::assertSame(20, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_with_cycle() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->cycle();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'cycle') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns bool instead of Boolean class) */
                self::assertTrue($defElem->getArg()?->getBoolean()?->getBoolval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_with_increment() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->incrementBy(10);

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'increment') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                self::assertSame(10, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_with_max_value() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->maxValue(9999999);

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'maxvalue') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                self::assertSame(9999999, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_with_min_value() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->minValue(1);

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'minvalue') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                self::assertSame(1, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_with_no_cycle() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->noCycle();

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'cycle') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns bool instead of Boolean class) */
                self::assertFalse($defElem->getArg()?->getBoolean()?->getBoolval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_sequence_with_schema() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq', 'public');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        self::assertSame('user_id_seq', $ast->getSequence()->getRelname());
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        self::assertSame('public', $ast->getSequence()->getSchemaname());
    }

    public function test_create_sequence_with_start_value() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->startWith(100);

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        self::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'start') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                self::assertSame(100, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        self::assertTrue($optionFound);
    }

    public function test_create_temporary_sequence() : void
    {
        $builder = CreateSequenceBuilder::createTemporary()
            ->sequence('temp_seq');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        self::assertSame('t', $ast->getSequence()->getRelpersistence());
    }

    public function test_create_unlogged_sequence() : void
    {
        $builder = CreateSequenceBuilder::createUnlogged()
            ->sequence('fast_seq');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        self::assertSame('u', $ast->getSequence()->getRelpersistence());
    }

    public function test_immutability() : void
    {
        $original = CreateSequenceBuilder::create()->sequence('test_seq');
        $modified = $original->startWith(100);

        self::assertEmpty($original->toAst()->getOptions());
        self::assertNotEmpty($modified->toAst()->getOptions());
    }

    public function test_simple_create_sequence() : void
    {
        $builder = CreateSequenceBuilder::create()
            ->sequence('user_id_seq');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        self::assertSame('user_id_seq', $ast->getSequence()->getRelname());
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        self::assertSame('p', $ast->getSequence()->getRelpersistence());
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\CreateSequence;

use Flow\PostgreSql\Protobuf\AST\CreateSeqStmt;
use Flow\PostgreSql\QueryBuilder\Schema\CreateSequence\CreateSequenceBuilder;
use PHPUnit\Framework\TestCase;

final class CreateSequenceBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_create_sequence_if_not_exists(): void
    {
        $builder = CreateSequenceBuilder::createIfNotExists()->sequence('user_id_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertTrue($ast->getIfNotExists());
    }

    public function test_create_sequence_no_max_value(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->noMaxValue();

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'maxvalue') {
                $optionFound = true;
                static::assertFalse($defElem->hasArg());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_no_min_value(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->noMinValue();

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'minvalue') {
                $optionFound = true;
                static::assertFalse($defElem->hasArg());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_owned_by(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->ownedBy('users', 'id');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'owned_by') {
                $optionFound = true;
                static::assertTrue($defElem->hasArg());
                $list = $defElem->getArg()?->getList();
                static::assertNotNull($list);
                static::assertCount(2, $list->getItems());
                static::assertSame('users', $list->getItems()[0]->getString()?->getSval());
                static::assertSame('id', $list->getItems()[1]->getString()?->getSval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_owned_by_none(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->ownedByNone();

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'owned_by') {
                $optionFound = true;
                static::assertTrue($defElem->hasArg());
                $list = $defElem->getArg()?->getList();
                static::assertNotNull($list);
                static::assertCount(1, $list->getItems());
                static::assertSame('none', $list->getItems()[0]->getString()?->getSval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_owned_by_with_schema(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->ownedBy('public.users', 'id');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'owned_by') {
                $optionFound = true;
                static::assertTrue($defElem->hasArg());
                $list = $defElem->getArg()?->getList();
                static::assertNotNull($list);
                static::assertCount(3, $list->getItems());
                static::assertSame('public', $list->getItems()[0]->getString()?->getSval());
                static::assertSame('users', $list->getItems()[1]->getString()?->getSval());
                static::assertSame('id', $list->getItems()[2]->getString()?->getSval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_with_all_options(): void
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

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertCount(7, $ast->getOptions());
    }

    public function test_create_sequence_with_as_type(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->asType('bigint');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'as') {
                $optionFound = true;
                static::assertTrue($defElem->hasArg());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_with_cache(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->cache(20);

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'cache') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                static::assertSame(20, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_with_cycle(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->cycle();

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'cycle') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns bool instead of Boolean class) */
                static::assertTrue($defElem->getArg()?->getBoolean()?->getBoolval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_with_increment(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->incrementBy(10);

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'increment') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                static::assertSame(10, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_with_max_value(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->maxValue(9999999);

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'maxvalue') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                static::assertSame(9999999, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_with_min_value(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->minValue(1);

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'minvalue') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                static::assertSame(1, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_with_no_cycle(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->noCycle();

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'cycle') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns bool instead of Boolean class) */
                static::assertFalse($defElem->getArg()?->getBoolean()?->getBoolval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_with_schema(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq', 'public');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        static::assertSame('user_id_seq', $ast->getSequence()->getRelname());
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        static::assertSame('public', $ast->getSequence()->getSchemaname());
    }

    public function test_create_sequence_with_start_value(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq')->startWith(100);

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'start') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                static::assertSame(100, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_temporary_sequence(): void
    {
        $builder = CreateSequenceBuilder::createTemporary()->sequence('temp_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        static::assertSame('t', $ast->getSequence()->getRelpersistence());
    }

    public function test_create_unlogged_sequence(): void
    {
        $builder = CreateSequenceBuilder::createUnlogged()->sequence('fast_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        static::assertSame('u', $ast->getSequence()->getRelpersistence());
    }

    public function test_immutability(): void
    {
        $original = CreateSequenceBuilder::create()->sequence('test_seq');
        $modified = $original->startWith(100);

        static::assertEmpty($original->toAst()->getOptions());
        static::assertNotEmpty($modified->toAst()->getOptions());
    }

    public function test_simple_create_sequence(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        static::assertSame('user_id_seq', $ast->getSequence()->getRelname());
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        static::assertSame('p', $ast->getSequence()->getRelpersistence());
    }
}

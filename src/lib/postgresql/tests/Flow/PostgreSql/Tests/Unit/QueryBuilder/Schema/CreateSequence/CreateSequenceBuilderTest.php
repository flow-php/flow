<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\CreateSequence;

use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\CreateSeqStmt;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\QueryBuilder\Schema\CreateSequence\CreateSequenceBuilder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\Types\DSL\type_instance_of;

final class CreateSequenceBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
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

            if ($defElem !== null && $defElem->getDefname() === 'maxvalue') {
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

            if ($defElem !== null && $defElem->getDefname() === 'minvalue') {
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

            if ($defElem !== null && $defElem->getDefname() === 'owned_by') {
                $optionFound = true;
                static::assertTrue($defElem->hasArg());
                $list = $defElem->getArg()?->getList();
                static::assertNotNull($list);
                $items = iterator_to_array($list->getItems());
                static::assertCount(2, $items);
                $first = $items[0];
                $second = $items[1];
                static::assertInstanceOf(Node::class, $first);
                static::assertInstanceOf(Node::class, $second);
                $firstString = $first->getString();
                $secondString = $second->getString();
                static::assertInstanceOf(PBString::class, $firstString);
                static::assertInstanceOf(PBString::class, $secondString);
                static::assertSame('users', $firstString->getSval());
                static::assertSame('id', $secondString->getSval());
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

            if ($defElem !== null && $defElem->getDefname() === 'owned_by') {
                $optionFound = true;
                static::assertTrue($defElem->hasArg());
                $list = $defElem->getArg()?->getList();
                static::assertNotNull($list);
                $items = iterator_to_array($list->getItems());
                static::assertCount(1, $items);
                $first = $items[0];
                static::assertInstanceOf(Node::class, $first);
                $firstString = $first->getString();
                static::assertInstanceOf(PBString::class, $firstString);
                static::assertSame('none', $firstString->getSval());
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

            if ($defElem !== null && $defElem->getDefname() === 'owned_by') {
                $optionFound = true;
                static::assertTrue($defElem->hasArg());
                $list = $defElem->getArg()?->getList();
                static::assertNotNull($list);
                $items = iterator_to_array($list->getItems());
                static::assertCount(3, $items);
                $first = $items[0];
                $second = $items[1];
                $third = $items[2];
                static::assertInstanceOf(Node::class, $first);
                static::assertInstanceOf(Node::class, $second);
                static::assertInstanceOf(Node::class, $third);
                $firstString = $first->getString();
                $secondString = $second->getString();
                $thirdString = $third->getString();
                static::assertInstanceOf(PBString::class, $firstString);
                static::assertInstanceOf(PBString::class, $secondString);
                static::assertInstanceOf(PBString::class, $thirdString);
                static::assertSame('public', $firstString->getSval());
                static::assertSame('users', $secondString->getSval());
                static::assertSame('id', $thirdString->getSval());
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

            if ($defElem !== null && $defElem->getDefname() === 'as') {
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

            if ($defElem !== null && $defElem->getDefname() === 'cache') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(20, $integer->getIval());
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

            if ($defElem !== null && $defElem->getDefname() === 'cycle') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $boolean = type_instance_of(Boolean::class)->assert($arg->getBoolean());
                static::assertTrue($boolean->getBoolval());
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

            if ($defElem !== null && $defElem->getDefname() === 'increment') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(10, $integer->getIval());
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

            if ($defElem !== null && $defElem->getDefname() === 'maxvalue') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(9999999, $integer->getIval());
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

            if ($defElem !== null && $defElem->getDefname() === 'minvalue') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(1, $integer->getIval());
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

            if ($defElem !== null && $defElem->getDefname() === 'cycle') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $boolean = type_instance_of(Boolean::class)->assert($arg->getBoolean());
                static::assertFalse($boolean->getBoolval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_sequence_with_schema(): void
    {
        $builder = CreateSequenceBuilder::create()->sequence('user_id_seq', 'public');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        $sequence = $ast->getSequence();
        static::assertInstanceOf(RangeVar::class, $sequence);
        static::assertSame('user_id_seq', $sequence->getRelname());
        static::assertSame('public', $sequence->getSchemaname());
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

            if ($defElem !== null && $defElem->getDefname() === 'start') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(100, $integer->getIval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_create_temporary_sequence(): void
    {
        $builder = CreateSequenceBuilder::createTemporary()->sequence('temp_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        $sequence = $ast->getSequence();
        static::assertInstanceOf(RangeVar::class, $sequence);
        static::assertSame('t', $sequence->getRelpersistence());
    }

    public function test_create_unlogged_sequence(): void
    {
        $builder = CreateSequenceBuilder::createUnlogged()->sequence('fast_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSeqStmt::class, $ast);
        $sequence = $ast->getSequence();
        static::assertInstanceOf(RangeVar::class, $sequence);
        static::assertSame('u', $sequence->getRelpersistence());
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
        $sequence = $ast->getSequence();
        static::assertInstanceOf(RangeVar::class, $sequence);
        static::assertSame('user_id_seq', $sequence->getRelname());
        static::assertSame('p', $sequence->getRelpersistence());
    }
}

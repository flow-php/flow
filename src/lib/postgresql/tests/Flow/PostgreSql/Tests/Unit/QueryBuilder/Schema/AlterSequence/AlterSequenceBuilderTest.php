<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\Protobuf\AST\AlterSeqStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableType;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\Schema\AlterSequence\AlterSequenceBuilder;
use PHPUnit\Framework\TestCase;

final class AlterSequenceBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_sequence_as_type(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->asType('smallint');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_cache(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->cache(20);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_cycle(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->cycle();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_if_exists(): void
    {
        $builder = AlterSequenceBuilder::ifExists()->sequence('user_id_seq')->incrementBy(10);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_alter_sequence_increment_by(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->incrementBy(10);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);
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

    public function test_alter_sequence_max_value(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->maxValue(9999999);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_min_value(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->minValue(1);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_no_cycle(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->noCycle();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_no_max_value(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->noMaxValue();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_no_min_value(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->noMinValue();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_owned_by(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->ownedBy('users', 'id');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'owned_by') {
                $optionFound = true;
                $list = $defElem->getArg()?->getList();
                static::assertNotNull($list);
                static::assertCount(2, $list->getItems());
                static::assertSame('users', $list->getItems()[0]->getString()?->getSval());
                static::assertSame('id', $list->getItems()[1]->getString()?->getSval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_alter_sequence_owned_by_none(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->ownedByNone();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'owned_by') {
                $optionFound = true;
                $list = $defElem->getArg()?->getList();
                static::assertNotNull($list);
                static::assertCount(1, $list->getItems());
                static::assertSame('none', $list->getItems()[0]->getString()?->getSval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_alter_sequence_owner_to(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->ownerTo('new_owner');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_SEQUENCE, $ast->getObjtype());
        static::assertSame('user_id_seq', $ast->getRelation()?->getRelname());
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_ChangeOwner, $ast->getCmds()[0]->getAlterTableCmd()?->getSubtype());
    }

    public function test_alter_sequence_owner_to_if_exists(): void
    {
        $builder = AlterSequenceBuilder::ifExists()->sequence('user_id_seq')->ownerTo('new_owner');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_alter_sequence_rename_to(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('old_seq')->renameTo('new_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(RenameStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_SEQUENCE, $ast->getRenameType());
        static::assertSame('old_seq', $ast->getRelation()?->getRelname());
        static::assertSame('new_seq', $ast->getNewname());
    }

    public function test_alter_sequence_rename_to_if_exists(): void
    {
        $builder = AlterSequenceBuilder::ifExists()->sequence('old_seq')->renameTo('new_seq');

        $ast = $builder->toAst();

        static::assertInstanceOf(RenameStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_alter_sequence_restart(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->restart();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        static::assertSame('user_id_seq', $ast->getSequence()->getRelname());
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'restart') {
                $optionFound = true;
                static::assertFalse($defElem->hasArg());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_alter_sequence_restart_with_value(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->restartWith(1000);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem?->getDefname() === 'restart') {
                $optionFound = true;
                /** @phpstan-ignore method.nonObject (protobuf PHPDoc incorrectly returns int instead of Integer class) */
                static::assertSame(1000, $defElem->getArg()?->getInteger()?->getIval());
            }
        }
        static::assertTrue($optionFound);
    }

    public function test_alter_sequence_set_logged(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->setLogged();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_SEQUENCE, $ast->getObjtype());
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_SetLogged, $ast->getCmds()[0]->getAlterTableCmd()?->getSubtype());
    }

    public function test_alter_sequence_set_logged_if_exists(): void
    {
        $builder = AlterSequenceBuilder::ifExists()->sequence('user_id_seq')->setLogged();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_alter_sequence_set_schema(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->setSchema('new_schema');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterObjectSchemaStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_SEQUENCE, $ast->getObjectType());
        static::assertSame('user_id_seq', $ast->getRelation()?->getRelname());
        static::assertSame('new_schema', $ast->getNewschema());
    }

    public function test_alter_sequence_set_schema_if_exists(): void
    {
        $builder = AlterSequenceBuilder::ifExists()->sequence('user_id_seq')->setSchema('new_schema');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterObjectSchemaStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_alter_sequence_set_unlogged(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->setUnlogged();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_SEQUENCE, $ast->getObjtype());
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_SetUnLogged, $ast->getCmds()[0]->getAlterTableCmd()?->getSubtype());
    }

    public function test_alter_sequence_start_with(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->startWith(100);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_with_multiple_options(): void
    {
        $builder = AlterSequenceBuilder::create()
            ->sequence('user_id_seq')
            ->incrementBy(10)
            ->minValue(1)
            ->maxValue(1000000)
            ->cache(5);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);
        static::assertCount(4, $ast->getOptions());
    }

    public function test_alter_sequence_with_schema(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq', 'public')->incrementBy(10);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        static::assertSame('user_id_seq', $ast->getSequence()->getRelname());
        /** @phpstan-ignore method.nonObject (protobuf returns nullable but we know it's set) */
        static::assertSame('public', $ast->getSequence()->getSchemaname());
    }

    public function test_immutability(): void
    {
        $original = AlterSequenceBuilder::create()->sequence('test_seq');
        $modified = $original->incrementBy(10);

        static::assertEmpty($original->toAst()->getOptions());
        static::assertNotEmpty($modified->toAst()->getOptions());
    }
}

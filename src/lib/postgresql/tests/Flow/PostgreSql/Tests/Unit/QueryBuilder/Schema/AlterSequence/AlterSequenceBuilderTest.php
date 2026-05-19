<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\AlterSequence;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\Protobuf\AST\AlterSeqStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableType;
use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\Schema\AlterSequence\AlterSequenceBuilder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\Types\DSL\type_instance_of;

final class AlterSequenceBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
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

            if ($defElem !== null && $defElem->getDefname() === 'as') {
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

    public function test_alter_sequence_cycle(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->cycle();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_max_value(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->maxValue(9999999);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_min_value(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->minValue(1);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_no_cycle(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->noCycle();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_no_max_value(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->noMaxValue();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_no_min_value(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->noMinValue();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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

    public function test_alter_sequence_owned_by(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->ownedBy('users', 'id');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem !== null && $defElem->getDefname() === 'owned_by') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $list = $arg->getList();
                static::assertNotNull($list);
                $items = $list->getItems();
                static::assertCount(2, $items);
                $first = type_instance_of(Node::class)->assert($items[0]);
                static::assertSame('users', $first->getString()?->getSval());
                $second = type_instance_of(Node::class)->assert($items[1]);
                static::assertSame('id', $second->getString()?->getSval());
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

            if ($defElem !== null && $defElem->getDefname() === 'owned_by') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $list = $arg->getList();
                static::assertNotNull($list);
                $items = $list->getItems();
                static::assertCount(1, $items);
                $first = type_instance_of(Node::class)->assert($items[0]);
                static::assertSame('none', $first->getString()?->getSval());
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
        $cmds = $ast->getCmds();
        static::assertCount(1, $cmds);
        $cmd = type_instance_of(Node::class)->assert($cmds[0]);
        static::assertSame(AlterTableType::AT_ChangeOwner, $cmd->getAlterTableCmd()?->getSubtype());
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
        $sequence = $ast->getSequence();
        static::assertNotNull($sequence);
        static::assertSame('user_id_seq', $sequence->getRelname());
        static::assertNotEmpty($ast->getOptions());

        $optionFound = false;

        foreach ($ast->getOptions() as $option) {
            $defElem = $option->getDefElem();

            if ($defElem !== null && $defElem->getDefname() === 'restart') {
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

            if ($defElem !== null && $defElem->getDefname() === 'restart') {
                $optionFound = true;
                $arg = $defElem->getArg();
                static::assertNotNull($arg);
                $integer = type_instance_of(Integer::class)->assert($arg->getInteger());
                static::assertSame(1000, $integer->getIval());
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
        $cmds = $ast->getCmds();
        static::assertCount(1, $cmds);
        $cmd = type_instance_of(Node::class)->assert($cmds[0]);
        static::assertSame(AlterTableType::AT_SetLogged, $cmd->getAlterTableCmd()?->getSubtype());
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
        $cmds = $ast->getCmds();
        static::assertCount(1, $cmds);
        $cmd = type_instance_of(Node::class)->assert($cmds[0]);
        static::assertSame(AlterTableType::AT_SetUnLogged, $cmd->getAlterTableCmd()?->getSubtype());
    }

    public function test_alter_sequence_start_with(): void
    {
        $builder = AlterSequenceBuilder::create()->sequence('user_id_seq')->startWith(100);

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterSeqStmt::class, $ast);

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
        $sequence = $ast->getSequence();
        static::assertNotNull($sequence);
        static::assertSame('user_id_seq', $sequence->getRelname());
        static::assertSame('public', $sequence->getSchemaname());
    }

    public function test_immutability(): void
    {
        $original = AlterSequenceBuilder::create()->sequence('test_seq');
        $modified = $original->incrementBy(10);

        static::assertEmpty($original->toAst()->getOptions());
        static::assertNotEmpty($modified->toAst()->getOptions());
    }
}

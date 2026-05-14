<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Constraint;

use Flow\PostgreSql\Protobuf\AST\Constraint;
use Flow\PostgreSql\Protobuf\AST\ConstrType;
use Flow\PostgreSql\Protobuf\AST\IndexElem;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBList;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Schema\Constraint\ExcludeConstraint;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\literal;
use function Flow\Types\DSL\type_instance_of;

final class ExcludeConstraintTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_deferrable_initially_deferred_emits_flags_on_ast(): void
    {
        $ast = ExcludeConstraint::create('btree')->element(col('room_id'), '=')->deferrable(true)->toAst();

        static::assertTrue($ast->getDeferrable());
        static::assertTrue($ast->getInitdeferred());
    }

    public function test_deferrable_initially_immediate_does_not_set_initdeferred(): void
    {
        $ast = ExcludeConstraint::create('btree')->element(col('room_id'), '=')->deferrable()->toAst();

        static::assertTrue($ast->getDeferrable());
        static::assertFalse($ast->getInitdeferred());
    }

    public function test_element_expression_uses_index_elem_expr_field(): void
    {
        $ast = ExcludeConstraint::create('gist')
            ->element(func('tsrange', [col('start'), col('finish')]), '&&')
            ->toAst();

        $exclusions = $ast->getExclusions();
        $pairList = type_instance_of(Node::class)->assert($exclusions[0])->getList();
        static::assertNotNull($pairList);
        $items = \iterator_to_array($pairList->getItems());

        $indexElem = type_instance_of(Node::class)->assert($items[0])->getIndexElem();
        static::assertNotNull($indexElem);
        static::assertSame('', $indexElem->getName());
        static::assertNotNull($indexElem->getExpr());
    }

    public function test_exclude_constraint_with_btree(): void
    {
        $constraint = ExcludeConstraint::create('btree')->element(col('id'), '=');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_EXCLUSION, $ast->getContype());
        static::assertSame('btree', $ast->getAccessMethod());
    }

    public function test_exclude_constraint_with_multiple_elements(): void
    {
        $constraint = ExcludeConstraint::create('gist')->element(col('room_id'), '=')->element(col('during'), '&&');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_EXCLUSION, $ast->getContype());
        static::assertCount(2, $ast->getExclusions());
    }

    public function test_exclude_constraint_with_name(): void
    {
        $constraint = ExcludeConstraint::create()->element(col('room_id'), '=')->name('exc_room_booking');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('exc_room_booking', $ast->getConname());
    }

    public function test_exclude_constraint_with_where_clause(): void
    {
        $constraint = ExcludeConstraint::create()
            ->element(col('room_id'), '=')
            ->where(eq(col('active'), literal(true)));

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertTrue($ast->hasWhereClause());
    }

    public function test_exclude_with_all_options(): void
    {
        $constraint = ExcludeConstraint::create('gist')
            ->name('exc_booking')
            ->element(col('room_id'), '=')
            ->element(col('period'), '&&')
            ->where(eq(col('cancelled'), literal(false)));

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame('exc_booking', $ast->getConname());
        static::assertSame('gist', $ast->getAccessMethod());
        static::assertCount(2, $ast->getExclusions());
        static::assertTrue($ast->hasWhereClause());
    }

    public function test_exclusions_are_list_of_pairs_of_index_elem_and_operator_list(): void
    {
        $ast = ExcludeConstraint::create('btree')->element(col('room_id'), '=')->toAst();

        $exclusions = $ast->getExclusions();

        static::assertCount(1, $exclusions);

        $pairList = $exclusions[0]->getList();
        static::assertInstanceOf(PBList::class, $pairList);

        $items = \iterator_to_array($pairList->getItems());
        static::assertCount(2, $items);

        $indexElem = $items[0]->getIndexElem();
        static::assertInstanceOf(IndexElem::class, $indexElem);
        static::assertSame('room_id', $indexElem->getName());

        $operatorList = $items[1]->getList();
        static::assertInstanceOf(PBList::class, $operatorList);
        $operatorItems = \iterator_to_array($operatorList->getItems());
        static::assertCount(1, $operatorItems);
        $operatorString = $operatorItems[0]->getString();
        static::assertInstanceOf(PBString::class, $operatorString);
        static::assertSame('=', $operatorString->getSval());
    }

    public function test_immutability(): void
    {
        $original = ExcludeConstraint::create();
        $withName = $original->name('exc_test');

        static::assertNotSame($original, $withName);
        static::assertSame('', $original->toAst()->getConname());
        static::assertSame('exc_test', $withName->toAst()->getConname());
    }

    public function test_not_deferrable_by_default(): void
    {
        $ast = ExcludeConstraint::create('btree')->element(col('room_id'), '=')->toAst();

        static::assertFalse($ast->getDeferrable());
        static::assertFalse($ast->getInitdeferred());
    }

    public function test_simple_exclude_constraint(): void
    {
        $constraint = ExcludeConstraint::create()->element(col('room_id'), '=');

        $ast = $constraint->toAst();

        static::assertInstanceOf(Constraint::class, $ast);
        static::assertSame(ConstrType::CONSTR_EXCLUSION, $ast->getContype());
        static::assertSame('gist', $ast->getAccessMethod());
        static::assertCount(1, $ast->getExclusions());
    }
}

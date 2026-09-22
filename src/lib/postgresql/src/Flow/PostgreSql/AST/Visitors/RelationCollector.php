<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Visitors;

use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\Protobuf\AST\AlterExtensionContentsStmt;
use Flow\PostgreSql\Protobuf\AST\CommentStmt;
use Flow\PostgreSql\Protobuf\AST\CommonTableExpr;
use Flow\PostgreSql\Protobuf\AST\DeleteStmt;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\InsertStmt;
use Flow\PostgreSql\Protobuf\AST\IntoClause;
use Flow\PostgreSql\Protobuf\AST\LockingClause;
use Flow\PostgreSql\Protobuf\AST\MergeStmt;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\SecLabelStmt;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\UpdateStmt;

use function array_pop;
use function array_slice;
use function count;
use function in_array;
use function iterator_to_array;

final class RelationCollector implements NodeVisitor
{
    private const array RELATION_KINDS = [
        ObjectType::OBJECT_TABLE,
        ObjectType::OBJECT_VIEW,
        ObjectType::OBJECT_MATVIEW,
        ObjectType::OBJECT_FOREIGN_TABLE,
        ObjectType::OBJECT_SEQUENCE,
        ObjectType::OBJECT_INDEX,
    ];

    /**
     * Objects named "relation name + own name", e.g. DROP TRIGGER trg ON s.t → [s, t, trg].
     */
    private const array RELATION_MEMBER_KINDS = [
        ObjectType::OBJECT_TRIGGER,
        ObjectType::OBJECT_RULE,
        ObjectType::OBJECT_POLICY,
        ObjectType::OBJECT_COLUMN,
        ObjectType::OBJECT_TABCONSTRAINT,
    ];

    /**
     * @var list<RangeVar>
     */
    private array $rangeVars = [];

    /**
     * @var list<array{names: list<string>, recursive: bool, entered: int, inCte: bool}>
     */
    private array $scopes = [];

    /**
     * @var list<RangeVar>
     */
    private array $targets = [];

    public static function nodeClasses(): array
    {
        return [
            SelectStmt::class,
            InsertStmt::class,
            UpdateStmt::class,
            DeleteStmt::class,
            MergeStmt::class,
            DropStmt::class,
            CommentStmt::class,
            SecLabelStmt::class,
            AlterExtensionContentsStmt::class,
            CommonTableExpr::class,
            IntoClause::class,
            LockingClause::class,
            RangeVar::class,
        ];
    }

    public function enter(object $node): ?int
    {
        if ($node instanceof LockingClause) {
            return NodeVisitor::DONT_TRAVERSE_CHILDREN;
        }

        if ($node instanceof IntoClause) {
            $rel = $node->getRel();

            if ($rel !== null) {
                $this->targets[] = $rel;
            }

            return null;
        }

        if ($node instanceof CommonTableExpr) {
            $scope = count($this->scopes) - 1;
            $this->scopes[$scope]['entered']++;
            $this->scopes[$scope]['inCte'] = true;

            return null;
        }

        if ($node instanceof RangeVar) {
            if (in_array($node, $this->targets, true) || $node->getSchemaname() !== '') {
                $this->rangeVars[] = $node;

                return null;
            }

            foreach ($this->scopes as $scope) {
                if (in_array(
                    $node->getRelname(),
                    $scope['inCte'] && !$scope['recursive']
                        ? array_slice($scope['names'], 0, $scope['entered'] - 1)
                        : $scope['names'],
                    true,
                )) {
                    return null;
                }
            }

            $this->rangeVars[] = $node;

            return null;
        }

        $objectType = null;
        $objects = [];

        if ($node instanceof DropStmt) {
            $objectType = $node->getRemoveType();
            $objects = iterator_to_array($node->getObjects(), false);
        }

        if (
            $node instanceof CommentStmt
            || $node instanceof SecLabelStmt
            || $node instanceof AlterExtensionContentsStmt
        ) {
            $objectType = $node->getObjtype();
            $objects = [$node->getObject()];
        }

        $member = in_array($objectType, self::RELATION_MEMBER_KINDS, true);

        if ($member || in_array($objectType, self::RELATION_KINDS, true)) {
            foreach ($objects as $object) {
                $names = [];

                foreach ($object?->getList()?->getItems() ?? [] as $item) {
                    $name = $item->getString()?->getSval();

                    if ($name !== null) {
                        $names[] = $name;
                    }
                }

                if ($member) {
                    array_pop($names);
                }

                $relname = array_pop($names);

                if ($relname === null) {
                    continue;
                }

                $this->rangeVars[] = new RangeVar([
                    'relname' => $relname,
                    'schemaname' => array_pop($names) ?? '',
                    'catalogname' => array_pop($names) ?? '',
                ]);
            }

            return null;
        }

        if (
            $node instanceof InsertStmt
            || $node instanceof UpdateStmt
            || $node instanceof DeleteStmt
            || $node instanceof MergeStmt
        ) {
            $relation = $node->getRelation();

            if ($relation !== null) {
                $this->targets[] = $relation;
            }
        }

        if (
            $node instanceof SelectStmt
            || $node instanceof InsertStmt
            || $node instanceof UpdateStmt
            || $node instanceof DeleteStmt
            || $node instanceof MergeStmt
        ) {
            $with = $node->getWithClause();

            if ($with !== null) {
                $names = [];

                foreach ($with->getCtes() as $cte) {
                    $names[] = $cte->getCommonTableExpr()?->getCtename() ?? '';
                }

                $this->scopes[] = [
                    'names' => $names,
                    'recursive' => $with->getRecursive(),
                    'entered' => 0,
                    'inCte' => false,
                ];
            }
        }

        return null;
    }

    /**
     * @return list<RangeVar>
     */
    public function getRangeVars(): array
    {
        return $this->rangeVars;
    }

    public function leave(object $node): ?int
    {
        if ($node instanceof CommonTableExpr) {
            $this->scopes[count($this->scopes) - 1]['inCte'] = false;

            return null;
        }

        if (
            (
                $node instanceof SelectStmt
                || $node instanceof InsertStmt
                || $node instanceof UpdateStmt
                || $node instanceof DeleteStmt
                || $node instanceof MergeStmt
            )
            && $node->getWithClause() !== null
        ) {
            array_pop($this->scopes);
        }

        return null;
    }

    public function reset(): void
    {
        $this->rangeVars = [];
        $this->scopes = [];
        $this->targets = [];
    }
}

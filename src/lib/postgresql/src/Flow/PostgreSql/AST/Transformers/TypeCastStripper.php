<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\Protobuf\AST\A_ArrayExpr;
use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Indirection;
use Flow\PostgreSql\Protobuf\AST\BooleanTest;
use Flow\PostgreSql\Protobuf\AST\BoolExpr;
use Flow\PostgreSql\Protobuf\AST\CaseExpr;
use Flow\PostgreSql\Protobuf\AST\CaseWhen;
use Flow\PostgreSql\Protobuf\AST\CoalesceExpr;
use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\MinMaxExpr;
use Flow\PostgreSql\Protobuf\AST\NamedArgExpr;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\NullTest;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\Protobuf\AST\RowExpr;
use Flow\PostgreSql\Protobuf\AST\SubLink;
use Flow\PostgreSql\Protobuf\AST\XmlExpr;

/**
 * Strips TypeCast wrappers from a protobuf expression AST.
 *
 * PostgreSQL's pg_get_expr() returns expressions with explicit casts added
 * (e.g. 'english'::regconfig, name::text, 'A'::"char") even when the original
 * expression was written without them. Passing this modifier to
 * {@see \Flow\PostgreSql\ParsedQuery::traverse()} rewrites every TypeCast
 * wrapper in the tree to its inner arg, recursively, so that round-trip
 * comparison against user-defined expressions matches.
 */
final readonly class TypeCastStripper implements NodeModifier
{
    public static function nodeClasses(): array
    {
        return [
            ResTarget::class,
            A_Expr::class,
            FuncCall::class,
            BoolExpr::class,
            CoalesceExpr::class,
            NullTest::class,
            BooleanTest::class,
            CaseExpr::class,
            CaseWhen::class,
            RowExpr::class,
            A_ArrayExpr::class,
            A_Indirection::class,
            MinMaxExpr::class,
            NamedArgExpr::class,
            SubLink::class,
            XmlExpr::class,
        ];
    }

    public function modify(object $node, ModificationContext $context): int|object|null
    {
        match (true) {
            $node instanceof ResTarget => $this->stripResTarget($node),
            $node instanceof A_Expr => $this->stripAExpr($node),
            $node instanceof FuncCall => $this->stripFuncCall($node),
            $node instanceof BoolExpr => $this->stripBoolExpr($node),
            $node instanceof CoalesceExpr => $this->stripCoalesceExpr($node),
            $node instanceof NullTest => $this->stripNullTest($node),
            $node instanceof BooleanTest => $this->stripBooleanTest($node),
            $node instanceof CaseExpr => $this->stripCaseExpr($node),
            $node instanceof CaseWhen => $this->stripCaseWhen($node),
            $node instanceof RowExpr => $this->stripRowExpr($node),
            $node instanceof A_ArrayExpr => $this->stripArrayExpr($node),
            $node instanceof A_Indirection => $this->stripIndirection($node),
            $node instanceof MinMaxExpr => $this->stripMinMaxExpr($node),
            $node instanceof NamedArgExpr => $this->stripNamedArgExpr($node),
            $node instanceof SubLink => $this->stripSubLink($node),
            $node instanceof XmlExpr => $this->stripXmlExpr($node),
            default => throw new \LogicException(\sprintf(
                'TypeCastStripper dispatched for unhandled node type %s',
                $node::class,
            )),
        };

        return null;
    }

    private function stripAExpr(A_Expr $node): void
    {
        if ($node->getLexpr() !== null) {
            $node->setLexpr($this->unwrap($node->getLexpr()));
        }

        if ($node->getRexpr() !== null) {
            $node->setRexpr($this->unwrap($node->getRexpr()));
        }
    }

    private function stripArrayExpr(A_ArrayExpr $node): void
    {
        $node->setElements($this->unwrapAll($node->getElements()));
    }

    private function stripBooleanTest(BooleanTest $node): void
    {
        if ($node->getArg() !== null) {
            $node->setArg($this->unwrap($node->getArg()));
        }
    }

    private function stripBoolExpr(BoolExpr $node): void
    {
        $node->setArgs($this->unwrapAll($node->getArgs()));
    }

    private function stripCaseExpr(CaseExpr $node): void
    {
        if ($node->getArg() !== null) {
            $node->setArg($this->unwrap($node->getArg()));
        }

        if ($node->getDefresult() !== null) {
            $node->setDefresult($this->unwrap($node->getDefresult()));
        }
    }

    private function stripCaseWhen(CaseWhen $node): void
    {
        if ($node->getExpr() !== null) {
            $node->setExpr($this->unwrap($node->getExpr()));
        }

        if ($node->getResult() !== null) {
            $node->setResult($this->unwrap($node->getResult()));
        }
    }

    private function stripCoalesceExpr(CoalesceExpr $node): void
    {
        $node->setArgs($this->unwrapAll($node->getArgs()));
    }

    private function stripFuncCall(FuncCall $node): void
    {
        $node->setArgs($this->unwrapAll($node->getArgs()));
        $node->setAggOrder($this->unwrapAll($node->getAggOrder()));

        if ($node->getAggFilter() !== null) {
            $node->setAggFilter($this->unwrap($node->getAggFilter()));
        }
    }

    private function stripIndirection(A_Indirection $node): void
    {
        if ($node->getArg() !== null) {
            $node->setArg($this->unwrap($node->getArg()));
        }
    }

    private function stripMinMaxExpr(MinMaxExpr $node): void
    {
        $node->setArgs($this->unwrapAll($node->getArgs()));
    }

    private function stripNamedArgExpr(NamedArgExpr $node): void
    {
        if ($node->getArg() !== null) {
            $node->setArg($this->unwrap($node->getArg()));
        }
    }

    private function stripNullTest(NullTest $node): void
    {
        if ($node->getArg() !== null) {
            $node->setArg($this->unwrap($node->getArg()));
        }
    }

    private function stripResTarget(ResTarget $node): void
    {
        if ($node->getVal() !== null) {
            $node->setVal($this->unwrap($node->getVal()));
        }
    }

    private function stripRowExpr(RowExpr $node): void
    {
        $node->setArgs($this->unwrapAll($node->getArgs()));
    }

    private function stripSubLink(SubLink $node): void
    {
        if ($node->getTestexpr() !== null) {
            $node->setTestexpr($this->unwrap($node->getTestexpr()));
        }
    }

    private function stripXmlExpr(XmlExpr $node): void
    {
        $node->setArgs($this->unwrapAll($node->getArgs()));
        $node->setNamedArgs($this->unwrapAll($node->getNamedArgs()));
    }

    /**
     * Unwrap any TypeCast wrappers at the top of $node, returning the inner arg.
     * Stacked TypeCasts are fully collapsed.
     */
    private function unwrap(Node $node): Node
    {
        while ($node->hasTypeCast()) {
            $inner = $node->getTypeCast()?->getArg();

            if ($inner === null) {
                return $node;
            }

            $node = $inner;
        }

        return $node;
    }

    /**
     * @param iterable<Node> $nodes
     *
     * @return array<Node>
     */
    private function unwrapAll(iterable $nodes): array
    {
        $result = [];

        foreach ($nodes as $node) {
            $result[] = $this->unwrap($node);
        }

        return $result;
    }
}

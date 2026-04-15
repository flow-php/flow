<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use Flow\PostgreSql\AST\Transformers\TypeCastStripper;
use Flow\PostgreSql\{ParsedQuery, Parser};
use Flow\PostgreSql\Protobuf\AST\{Node, ParseResult, RawStmt, ResTarget, SelectStmt, SetOperation};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class ExpressionParser
{
    private const string EXPR_ALIAS = 'x';

    private const string EXPR_SUFFIX = ' AS ' . self::EXPR_ALIAS;

    private const string SELECT_PREFIX = 'SELECT ';

    private Parser $parser;

    public function __construct(
        private TypeCastStripper $stripper = new TypeCastStripper(),
    ) {
        $this->parser = new Parser();
    }

    /**
     * Normalize an expression by stripping implicit type casts that PostgreSQL adds.
     *
     * PostgreSQL's pg_get_expr() returns expressions with explicit casts like
     * `to_tsvector('english'::regconfig, name::text)` even though the original expression
     * was `to_tsvector('english', name)`. This method parses the expression, strips all
     * TypeCast nodes from the AST, and deparses back to produce a canonical form
     * that matches what a user would write in a generation expression, default, check
     * constraint, index predicate, etc.
     */
    public function normalize(string $expression) : string
    {
        $parsed = $this->parser->parse(self::SELECT_PREFIX . $expression . self::EXPR_SUFFIX);
        $parsed->traverse($this->stripper);

        return $this->stripSelectWrapper($parsed->deparse());
    }

    /**
     * Normalize an expression Node by stripping implicit type casts.
     *
     * Same canonicalization as normalize(), but starts from a Node already extracted
     * from another AST (e.g. a WHERE clause pulled from a CreateTrigStmt). Avoids the
     * deparse/re-parse round-trip that would otherwise be needed to go through the
     * string-based normalize() entry point.
     */
    public function normalizeNode(Node $node) : string
    {
        $parsed = new ParsedQuery($this->wrapInSelect($node));
        $parsed->traverse($this->stripper);

        return $this->stripSelectWrapper($parsed->deparse());
    }

    public function parse(string $expression) : Node
    {
        $parsed = $this->parser->parse(self::SELECT_PREFIX . $expression . self::EXPR_SUFFIX);

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $selectStmt = $stmts[0]->getStmt()?->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $targetList = $selectStmt->getTargetList();

        if ($targetList === null || \count($targetList) === 0) {
            throw InvalidAstException::invalidFieldValue('targetList', 'SelectStmt', 'expected at least one target');
        }

        $resTarget = $targetList[0]->getResTarget();

        if ($resTarget === null) {
            throw InvalidAstException::unexpectedNodeType('ResTarget', 'unknown');
        }

        $val = $resTarget->getVal();

        if ($val === null) {
            throw InvalidAstException::missingRequiredField('val', 'ResTarget');
        }

        return $val;
    }

    public function parseStatement(string $sql) : ParsedQuery
    {
        return $this->parser->parse($sql);
    }

    private function stripSelectWrapper(string $sql) : string
    {
        return \substr($sql, \strlen(self::SELECT_PREFIX), -\strlen(self::EXPR_SUFFIX));
    }

    private function wrapInSelect(Node $node) : ParseResult
    {
        $resTarget = new ResTarget();
        $resTarget->setName(self::EXPR_ALIAS);
        $resTarget->setVal($node);

        $resTargetNode = new Node();
        $resTargetNode->setResTarget($resTarget);

        $selectStmt = new SelectStmt();
        $selectStmt->setTargetList([$resTargetNode]);
        $selectStmt->setOp(SetOperation::SETOP_NONE);

        $stmtNode = new Node();
        $stmtNode->setSelectStmt($selectStmt);

        $rawStmt = new RawStmt();
        $rawStmt->setStmt($stmtNode);

        $parseResult = new ParseResult();
        $parseResult->setVersion(170007);
        $parseResult->setStmts([$rawStmt]);

        return $parseResult;
    }
}

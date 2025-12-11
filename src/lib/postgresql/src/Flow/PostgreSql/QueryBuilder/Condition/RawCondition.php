<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\{Boolean, Node};
use Flow\PostgreSql\QueryBuilder\Exception\{InvalidAstException, UnsupportedNodeException};

final readonly class RawCondition implements Condition
{
    public function __construct(
        private string $sql,
    ) {
    }

    public static function fromAConst(Node $node) : static
    {
        $aConst = $node->getAConst();

        if ($aConst === null) {
            throw InvalidAstException::unexpectedNodeType('A_Const', 'unknown');
        }

        if ($aConst->hasBoolval()) {
            $boolval = $aConst->getBoolval();
            \assert($boolval instanceof Boolean);

            return new self($boolval->getBoolval() ? 'true' : 'false');
        }

        throw UnsupportedNodeException::cannotReconstruct(self::class . ' from A_Const');
    }

    public static function fromAst(Node $node) : static
    {
        throw UnsupportedNodeException::cannotReconstruct(self::class);
    }

    public static function fromTypeCast(Node $node) : static
    {
        $typeCast = $node->getTypeCast();

        if ($typeCast === null) {
            throw InvalidAstException::unexpectedNodeType('TypeCast', 'unknown');
        }

        $typeName = $typeCast->getTypeName();

        if ($typeName === null) {
            throw InvalidAstException::missingRequiredField('type_name', 'TypeCast');
        }

        $names = $typeName->getNames();

        if ($names !== null) {
            foreach ($names as $nameNode) {
                $stringNode = $nameNode->getString();

                if ($stringNode !== null && $stringNode->getSval() === 'bool') {
                    $arg = $typeCast->getArg();

                    if ($arg !== null) {
                        $aConst = $arg->getAConst();

                        if ($aConst !== null) {
                            $sval = $aConst->getSval();

                            if ($sval !== null) {
                                return new self($sval->getSval());
                            }
                        }
                    }
                }
            }
        }

        throw UnsupportedNodeException::cannotReconstruct(self::class . ' from TypeCast');
    }

    public function and(Condition $other) : AndCondition
    {
        return new AndCondition($this, $other);
    }

    public function not() : NotCondition
    {
        return new NotCondition($this);
    }

    public function or(Condition $other) : OrCondition
    {
        return new OrCondition($this, $other);
    }

    public function toAst() : Node
    {
        $parser = new Parser();
        $parsed = $parser->parse("SELECT 1 WHERE {$this->sql}");

        $stmts = $parsed->raw()->getStmts();

        if (\count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('sql', 'RawCondition', 'Cannot parse SQL condition');
        }

        $firstStmt = $stmts[0];

        if (!$firstStmt->hasStmt()) {
            throw InvalidAstException::invalidFieldValue('sql', 'RawCondition', 'Statement does not have stmt field');
        }

        $stmt = $firstStmt->getStmt();

        if ($stmt === null) {
            throw InvalidAstException::missingRequiredField('stmt', 'RawPgQuery_Stmt');
        }

        if (!$stmt->hasSelectStmt()) {
            throw InvalidAstException::invalidFieldValue('sql', 'RawCondition', 'Expected SelectStmt');
        }

        $selectStmt = $stmt->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::missingRequiredField('select_stmt', 'Node');
        }

        if (!$selectStmt->hasWhereClause()) {
            throw InvalidAstException::invalidFieldValue('sql', 'RawCondition', 'WHERE clause is missing');
        }

        $whereClause = $selectStmt->getWhereClause();

        if ($whereClause === null) {
            throw InvalidAstException::missingRequiredField('where_clause', 'SelectStmt');
        }

        return $whereClause;
    }
}

<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Condition;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\Node;
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException, UnsupportedNodeException};

final readonly class RawCondition implements Condition
{
    public function __construct(
        private string $sql,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        throw UnsupportedNodeException::cannotReconstruct(self::class);
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

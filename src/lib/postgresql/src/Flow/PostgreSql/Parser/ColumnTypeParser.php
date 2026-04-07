<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Parser;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

final readonly class ColumnTypeParser
{
    public function parse(string $typeName) : ColumnType
    {
        if ($typeName === '') {
            throw new \InvalidArgumentException('Type name cannot be empty');
        }

        $parser = new Parser();
        $parsed = $parser->parse("SELECT NULL::{$typeName}");

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

        $typeCast = $val->getTypeCast();

        if ($typeCast === null) {
            throw InvalidAstException::unexpectedNodeType('TypeCast', 'unknown');
        }

        $typeNameAst = $typeCast->getTypeName();

        if ($typeNameAst === null) {
            throw InvalidAstException::missingRequiredField('typeName', 'TypeCast');
        }

        return ColumnType::fromAst($typeNameAst);
    }
}

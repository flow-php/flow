<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Protobuf\AST\{ColumnDef, ConstrType, Constraint, Node, RangeVar};
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

final readonly class ColumnDefinition
{
    /**
     * @param list<Constraint> $constraints
     */
    private function __construct(
        private string $name,
        private DataType $type,
        private bool $notNull = false,
        private ?Node $defaultValue = null,
        private ?string $identity = null,
        private ?Node $generatedExpression = null,
        private array $constraints = [],
    ) {
    }

    public static function create(string $name, DataType $type) : self
    {
        return new self($name, $type);
    }

    public function check(string $expression) : self
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_CHECK);
        $constraint->setRawExpr($this->parseExpression($expression));

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            [...$this->constraints, $constraint],
        );
    }

    public function default(bool|float|int|string|Expression|null $value) : self
    {
        $node = $value instanceof Expression
            ? $value->toAst()
            : $this->createLiteralNode($value);

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $node,
            $this->identity,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function defaultRaw(string $expression) : self
    {
        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->parseExpression($expression),
            $this->identity,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function generatedAs(string $expression) : self
    {
        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $this->parseExpression($expression),
            $this->constraints,
        );
    }

    public function identity(string $type = 'ALWAYS') : self
    {
        $identityChar = \strtoupper($type) === 'ALWAYS' ? 'a' : 'd';

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $identityChar,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function notNull() : self
    {
        return new self(
            $this->name,
            $this->type,
            true,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function nullable() : self
    {
        return new self(
            $this->name,
            $this->type,
            false,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            $this->constraints,
        );
    }

    public function primaryKey() : self
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_PRIMARY);

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            [...$this->constraints, $constraint],
        );
    }

    public function references(string $table, ?string $column = null, ?string $schema = null) : self
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_FOREIGN);

        $rangeVar = new RangeVar();
        $rangeVar->setRelname($table);

        if ($schema !== null) {
            $rangeVar->setSchemaname($schema);
        }

        $constraint->setPktable($rangeVar);

        if ($column !== null) {
            $constraint->setPkAttrs([$this->createStringNode($column)]);
        }

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            [...$this->constraints, $constraint],
        );
    }

    public function toAst() : ColumnDef
    {
        $columnDef = new ColumnDef();
        $columnDef->setColname($this->name);
        $columnDef->setTypeName($this->type->toAst());
        $columnDef->setIsLocal(true);

        if ($this->identity !== null) {
            $columnDef->setIdentity($this->identity);
        }

        $allConstraints = [];

        if ($this->notNull) {
            $notNullConstraint = new Constraint();
            $notNullConstraint->setContype(ConstrType::CONSTR_NOTNULL);
            $allConstraints[] = $notNullConstraint;
        }

        if ($this->defaultValue !== null) {
            $defaultConstraint = new Constraint();
            $defaultConstraint->setContype(ConstrType::CONSTR_DEFAULT);
            $defaultConstraint->setRawExpr($this->defaultValue);
            $allConstraints[] = $defaultConstraint;
        }

        foreach ($this->constraints as $constraint) {
            $allConstraints[] = $constraint;
        }

        if ($this->generatedExpression !== null) {
            $columnDef->setGenerated('s');

            $generatedConstraint = new Constraint();
            $generatedConstraint->setContype(ConstrType::CONSTR_GENERATED);
            $generatedConstraint->setGeneratedWhen('a');
            $generatedConstraint->setRawExpr($this->generatedExpression);

            $allConstraints[] = $generatedConstraint;
        }

        if ($allConstraints !== []) {
            $constraintNodes = [];

            foreach ($allConstraints as $constraint) {
                $node = new Node();
                $node->setConstraint($constraint);
                $constraintNodes[] = $node;
            }

            $columnDef->setConstraints($constraintNodes);
        }

        return $columnDef;
    }

    public function unique() : self
    {
        $constraint = new Constraint();
        $constraint->setContype(ConstrType::CONSTR_UNIQUE);

        return new self(
            $this->name,
            $this->type,
            $this->notNull,
            $this->defaultValue,
            $this->identity,
            $this->generatedExpression,
            [...$this->constraints, $constraint],
        );
    }

    private function createLiteralNode(bool|float|int|string|null $value) : Node
    {
        $parser = new Parser();

        $literal = match (true) {
            $value === null => 'NULL',
            \is_bool($value) => $value ? 'TRUE' : 'FALSE',
            \is_string($value) => "'" . \str_replace("'", "''", $value) . "'",
            default => (string) $value,
        };

        $parsed = $parser->parse("SELECT {$literal} AS x");

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $firstStmt = $stmts[0];
        $selectStmt = $firstStmt->getStmt()?->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $targetList = $selectStmt->getTargetList();

        if ($targetList === null || \count($targetList) === 0) {
            throw InvalidAstException::invalidFieldValue('targetList', 'SelectStmt', 'expected at least one target');
        }

        $firstTarget = $targetList[0];
        $resTarget = $firstTarget->getResTarget();

        if ($resTarget === null) {
            throw InvalidAstException::unexpectedNodeType('ResTarget', 'unknown');
        }

        $val = $resTarget->getVal();

        if ($val === null) {
            throw InvalidAstException::missingRequiredField('val', 'ResTarget');
        }

        return $val;
    }

    private function createStringNode(string $value) : Node
    {
        $str = new PBString();
        $str->setSval($value);

        $node = new Node();
        $node->setString($str);

        return $node;
    }

    private function parseExpression(string $expression) : Node
    {
        $parser = new Parser();
        $parsed = $parser->parse("SELECT {$expression} AS x");

        $stmts = $parsed->raw()->getStmts();

        if ($stmts === null || \count($stmts) === 0) {
            throw InvalidAstException::invalidFieldValue('stmts', 'ParseResult', 'expected at least one statement');
        }

        $firstStmt = $stmts[0];
        $selectStmt = $firstStmt->getStmt()?->getSelectStmt();

        if ($selectStmt === null) {
            throw InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown');
        }

        $targetList = $selectStmt->getTargetList();

        if ($targetList === null || \count($targetList) === 0) {
            throw InvalidAstException::invalidFieldValue('targetList', 'SelectStmt', 'expected at least one target');
        }

        $firstTarget = $targetList[0];
        $resTarget = $firstTarget->getResTarget();

        if ($resTarget === null) {
            throw InvalidAstException::unexpectedNodeType('ResTarget', 'unknown');
        }

        $val = $resTarget->getVal();

        if ($val === null) {
            throw InvalidAstException::missingRequiredField('val', 'ResTarget');
        }

        return $val;
    }
}

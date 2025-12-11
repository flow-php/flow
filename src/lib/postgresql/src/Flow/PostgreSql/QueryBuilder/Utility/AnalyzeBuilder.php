<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\{DefElem, Node, PBString, RangeVar, VacuumRelation as VacuumRelationAST, VacuumStmt};
use Flow\PostgreSql\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class AnalyzeBuilder implements AnalyzeFinalStep
{
    use AstToSql;

    /**
     * @param array<VacuumRelation> $relations
     */
    private function __construct(
        private array $relations = [],
        private bool $verbose = false,
        private bool $skipLocked = false,
    ) {
    }

    public static function create() : AnalyzeFinalStep
    {
        return new self();
    }

    public function skipLocked() : AnalyzeFinalStep
    {
        return new self(
            $this->relations,
            $this->verbose,
            true,
        );
    }

    public function table(string $table, string ...$columns) : AnalyzeFinalStep
    {
        return new self(
            [...$this->relations, new VacuumRelation($table, $columns)],
            $this->verbose,
            $this->skipLocked,
        );
    }

    public function tables(string ...$tables) : AnalyzeFinalStep
    {
        $relations = $this->relations;

        foreach ($tables as $table) {
            $relations[] = new VacuumRelation($table);
        }

        return new self(
            $relations,
            $this->verbose,
            $this->skipLocked,
        );
    }

    public function toAst() : VacuumStmt
    {
        $stmt = new VacuumStmt();
        $stmt->setIsVacuumcmd(false);

        $options = [];

        if ($this->verbose) {
            $options[] = $this->createDefElemBool('verbose');
        }

        if ($this->skipLocked) {
            $options[] = $this->createDefElemBool('skip_locked');
        }

        if ($options !== []) {
            $stmt->setOptions($options);
        }

        $rels = [];

        foreach ($this->relations as $relation) {
            $identifier = QualifiedIdentifier::parse($relation->table);
            $rangeVar = new RangeVar();

            $schema = $identifier->schema();

            if ($schema !== null) {
                $rangeVar->setSchemaname($schema);
            }

            $rangeVar->setRelname($identifier->name());
            $rangeVar->setInh(true);
            $rangeVar->setRelpersistence('p');

            $vacuumRel = new VacuumRelationAST();
            $vacuumRel->setRelation($rangeVar);

            if ($relation->columns !== []) {
                $cols = [];

                foreach ($relation->columns as $column) {
                    $str = new PBString();
                    $str->setSval($column);
                    $colNode = new Node();
                    $colNode->setString($str);
                    $cols[] = $colNode;
                }

                $vacuumRel->setVaCols($cols);
            }

            $relNode = new Node();
            $relNode->setVacuumRelation($vacuumRel);
            $rels[] = $relNode;
        }

        if ($rels !== []) {
            $stmt->setRels($rels);
        }

        return $stmt;
    }

    public function verbose() : AnalyzeFinalStep
    {
        return new self(
            $this->relations,
            true,
            $this->skipLocked,
        );
    }

    private function createDefElemBool(string $name) : Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }
}

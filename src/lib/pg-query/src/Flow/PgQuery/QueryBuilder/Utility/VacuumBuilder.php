<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\{DefElem, Integer, Node, PBString, RangeVar, VacuumRelation as VacuumRelationAST, VacuumStmt};
use Flow\PgQuery\QueryBuilder\{AstToSql, QualifiedIdentifier};

final readonly class VacuumBuilder implements VacuumFinalStep
{
    use AstToSql;

    /**
     * @param array<VacuumRelation> $relations
     */
    private function __construct(
        private array $relations = [],
        private bool $full = false,
        private bool $freeze = false,
        private bool $verbose = false,
        private bool $analyze = false,
        private bool $disablePageSkipping = false,
        private bool $skipLocked = false,
        private ?IndexCleanup $indexCleanup = null,
        private ?bool $processMain = null,
        private ?bool $processToast = null,
        private ?bool $truncate = null,
        private ?int $parallel = null,
    ) {
    }

    public static function create() : VacuumFinalStep
    {
        return new self();
    }

    public function analyze() : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            $this->freeze,
            $this->verbose,
            true,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $this->parallel,
        );
    }

    public function disablePageSkipping() : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            true,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $this->parallel,
        );
    }

    public function freeze() : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            true,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $this->parallel,
        );
    }

    public function full() : VacuumFinalStep
    {
        return new self(
            $this->relations,
            true,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $this->parallel,
        );
    }

    public function indexCleanup(IndexCleanup $cleanup) : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $cleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $this->parallel,
        );
    }

    public function parallel(int $workers) : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $workers,
        );
    }

    public function processMain(bool $enabled) : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $enabled,
            $this->processToast,
            $this->truncate,
            $this->parallel,
        );
    }

    public function processToast(bool $enabled) : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $enabled,
            $this->truncate,
            $this->parallel,
        );
    }

    public function skipLocked() : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            true,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $this->parallel,
        );
    }

    public function table(string $table, string ...$columns) : VacuumFinalStep
    {
        return new self(
            [...$this->relations, new VacuumRelation($table, $columns)],
            $this->full,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $this->parallel,
        );
    }

    public function tables(string ...$tables) : VacuumFinalStep
    {
        $relations = $this->relations;

        foreach ($tables as $table) {
            $relations[] = new VacuumRelation($table);
        }

        return new self(
            $relations,
            $this->full,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $this->parallel,
        );
    }

    public function toAst() : VacuumStmt
    {
        $stmt = new VacuumStmt();
        $stmt->setIsVacuumcmd(true);

        $options = [];

        if ($this->full) {
            $options[] = $this->createDefElemBool('full');
        }

        if ($this->freeze) {
            $options[] = $this->createDefElemBool('freeze');
        }

        if ($this->verbose) {
            $options[] = $this->createDefElemBool('verbose');
        }

        if ($this->analyze) {
            $options[] = $this->createDefElemBool('analyze');
        }

        if ($this->disablePageSkipping) {
            $options[] = $this->createDefElemBool('disable_page_skipping');
        }

        if ($this->skipLocked) {
            $options[] = $this->createDefElemBool('skip_locked');
        }

        if ($this->indexCleanup !== null) {
            $options[] = $this->createDefElemString('index_cleanup', $this->indexCleanup->value);
        }

        if ($this->processMain !== null) {
            $options[] = $this->createDefElemInt('process_main', $this->processMain ? 1 : 0);
        }

        if ($this->processToast !== null) {
            $options[] = $this->createDefElemInt('process_toast', $this->processToast ? 1 : 0);
        }

        if ($this->truncate !== null) {
            $options[] = $this->createDefElemInt('truncate', $this->truncate ? 1 : 0);
        }

        if ($this->parallel !== null) {
            $options[] = $this->createDefElemInt('parallel', $this->parallel);
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

    public function truncate(bool $enabled) : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            $this->freeze,
            $this->verbose,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $enabled,
            $this->parallel,
        );
    }

    public function verbose() : VacuumFinalStep
    {
        return new self(
            $this->relations,
            $this->full,
            $this->freeze,
            true,
            $this->analyze,
            $this->disablePageSkipping,
            $this->skipLocked,
            $this->indexCleanup,
            $this->processMain,
            $this->processToast,
            $this->truncate,
            $this->parallel,
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

    private function createDefElemInt(string $name, int $value) : Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $int = new Integer();
        $int->setIval($value);

        $intNode = new Node();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $intNode->setInteger($int);
        $defElem->setArg($intNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }

    private function createDefElemString(string $name, string $value) : Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $str = new PBString();
        $str->setSval($value);

        $strNode = new Node();
        $strNode->setString($str);
        $defElem->setArg($strNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }
}

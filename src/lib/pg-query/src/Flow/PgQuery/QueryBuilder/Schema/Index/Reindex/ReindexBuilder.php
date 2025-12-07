<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\Reindex;

use Flow\PgQuery\Protobuf\AST\{DefElem, Node, PBString, RangeVar, ReindexObjectType, ReindexStmt};

final readonly class ReindexBuilder implements ReindexFinalStep
{
    private function __construct(
        private int $kind,
        private string $name,
        private bool $concurrent = false,
        private ?string $tablespace = null,
        private bool $verbose = false,
    ) {
    }

    public static function database(string $name) : ReindexFinalStep
    {
        return new self(ReindexObjectType::REINDEX_OBJECT_DATABASE, $name);
    }

    public static function index(string $name) : ReindexFinalStep
    {
        return new self(ReindexObjectType::REINDEX_OBJECT_INDEX, $name);
    }

    public static function schema(string $name) : ReindexFinalStep
    {
        return new self(ReindexObjectType::REINDEX_OBJECT_SCHEMA, $name);
    }

    public static function system(string $name) : ReindexFinalStep
    {
        return new self(ReindexObjectType::REINDEX_OBJECT_SYSTEM, $name);
    }

    public static function table(string $name) : ReindexFinalStep
    {
        return new self(ReindexObjectType::REINDEX_OBJECT_TABLE, $name);
    }

    public function concurrently() : ReindexFinalStep
    {
        return new self(
            $this->kind,
            $this->name,
            true,
            $this->tablespace,
            $this->verbose,
        );
    }

    public function tablespace(string $tablespace) : ReindexFinalStep
    {
        return new self(
            $this->kind,
            $this->name,
            $this->concurrent,
            $tablespace,
            $this->verbose,
        );
    }

    public function toAst() : ReindexStmt
    {
        $stmt = new ReindexStmt();
        $stmt->setKind($this->kind);

        if ($this->kind === ReindexObjectType::REINDEX_OBJECT_INDEX || $this->kind === ReindexObjectType::REINDEX_OBJECT_TABLE) {
            $rangeVar = new RangeVar();

            $parts = \explode('.', $this->name);

            if (\count($parts) === 2) {
                $rangeVar->setSchemaname($parts[0]);
                $rangeVar->setRelname($parts[1]);
            } else {
                $rangeVar->setRelname($this->name);
            }

            $rangeVar->setInh(true);
            $rangeVar->setRelpersistence('p');
            $stmt->setRelation($rangeVar);
        } else {
            $stmt->setName($this->name);
        }

        $params = [];

        if ($this->concurrent) {
            $params[] = $this->createDefElemBool('concurrently');
        }

        if ($this->tablespace !== null) {
            $params[] = $this->createDefElemString('tablespace', $this->tablespace);
        }

        if ($this->verbose) {
            $params[] = $this->createDefElemBool('verbose');
        }

        if ($params !== []) {
            $stmt->setParams($params);
        }

        return $stmt;
    }

    public function verbose() : ReindexFinalStep
    {
        return new self(
            $this->kind,
            $this->name,
            $this->concurrent,
            $this->tablespace,
            true,
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

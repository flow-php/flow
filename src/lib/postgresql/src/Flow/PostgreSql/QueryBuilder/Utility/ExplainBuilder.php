<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\ExplainStmt;
use Flow\PostgreSql\Protobuf\AST\InsertStmt;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\UpdateStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Delete\DeleteFinalStep;
use Flow\PostgreSql\QueryBuilder\Insert\InsertFinalStep;
use Flow\PostgreSql\QueryBuilder\Select\SelectFinalStep;
use Flow\PostgreSql\QueryBuilder\Update\UpdateFinalStep;

final readonly class ExplainBuilder implements ExplainFinalStep
{
    use AstToSql;

    private function __construct(
        private SelectFinalStep|InsertFinalStep|UpdateFinalStep|DeleteFinalStep $query,
        private bool $analyze = false,
        private bool $verbose = false,
        private ?bool $costs = null,
        private ?bool $settings = null,
        private ?bool $buffers = null,
        private ?bool $wal = null,
        private ?bool $timing = null,
        private ?bool $summary = null,
        private ?bool $memory = null,
        private ?ExplainFormat $format = null,
    ) {}

    public static function create(SelectFinalStep|InsertFinalStep|UpdateFinalStep|DeleteFinalStep $query): ExplainFinalStep
    {
        return new self($query);
    }

    public function analyze(): ExplainFinalStep
    {
        return new self(
            $this->query,
            true,
            $this->verbose,
            $this->costs,
            $this->settings,
            $this->buffers,
            $this->wal,
            $this->timing,
            $this->summary,
            $this->memory,
            $this->format,
        );
    }

    public function buffers(bool $enabled = true): ExplainFinalStep
    {
        return new self(
            $this->query,
            $this->analyze,
            $this->verbose,
            $this->costs,
            $this->settings,
            $enabled,
            $this->wal,
            $this->timing,
            $this->summary,
            $this->memory,
            $this->format,
        );
    }

    public function costs(bool $enabled = true): ExplainFinalStep
    {
        return new self(
            $this->query,
            $this->analyze,
            $this->verbose,
            $enabled,
            $this->settings,
            $this->buffers,
            $this->wal,
            $this->timing,
            $this->summary,
            $this->memory,
            $this->format,
        );
    }

    public function format(ExplainFormat $format): ExplainFinalStep
    {
        return new self(
            $this->query,
            $this->analyze,
            $this->verbose,
            $this->costs,
            $this->settings,
            $this->buffers,
            $this->wal,
            $this->timing,
            $this->summary,
            $this->memory,
            $format,
        );
    }

    public function memory(bool $enabled = true): ExplainFinalStep
    {
        return new self(
            $this->query,
            $this->analyze,
            $this->verbose,
            $this->costs,
            $this->settings,
            $this->buffers,
            $this->wal,
            $this->timing,
            $this->summary,
            $enabled,
            $this->format,
        );
    }

    public function settings(bool $enabled = true): ExplainFinalStep
    {
        return new self(
            $this->query,
            $this->analyze,
            $this->verbose,
            $this->costs,
            $enabled,
            $this->buffers,
            $this->wal,
            $this->timing,
            $this->summary,
            $this->memory,
            $this->format,
        );
    }

    public function summary(bool $enabled = true): ExplainFinalStep
    {
        return new self(
            $this->query,
            $this->analyze,
            $this->verbose,
            $this->costs,
            $this->settings,
            $this->buffers,
            $this->wal,
            $this->timing,
            $enabled,
            $this->memory,
            $this->format,
        );
    }

    public function timing(bool $enabled = true): ExplainFinalStep
    {
        return new self(
            $this->query,
            $this->analyze,
            $this->verbose,
            $this->costs,
            $this->settings,
            $this->buffers,
            $this->wal,
            $enabled,
            $this->summary,
            $this->memory,
            $this->format,
        );
    }

    public function toAst(): ExplainStmt
    {
        $stmt = new ExplainStmt();

        $ast = $this->query->toAst();

        if ($ast instanceof SelectStmt) {
            $queryNode = new Node(['select_stmt' => $ast]);
        } elseif ($ast instanceof InsertStmt) {
            $queryNode = new Node(['insert_stmt' => $ast]);
        } elseif ($ast instanceof UpdateStmt) {
            $queryNode = new Node(['update_stmt' => $ast]);
        } else {
            $queryNode = new Node(['delete_stmt' => $ast]);
        }

        $stmt->setQuery($queryNode);

        $options = [];

        if ($this->analyze) {
            $options[] = $this->createDefElemBool('analyze');
        }

        if ($this->verbose) {
            $options[] = $this->createDefElemBool('verbose');
        }

        if ($this->costs !== null) {
            $options[] = $this->createDefElemInt('costs', $this->costs ? 1 : 0);
        }

        if ($this->settings !== null) {
            $options[] = $this->createDefElemInt('settings', $this->settings ? 1 : 0);
        }

        if ($this->buffers !== null) {
            $options[] = $this->createDefElemInt('buffers', $this->buffers ? 1 : 0);
        }

        if ($this->wal !== null) {
            $options[] = $this->createDefElemInt('wal', $this->wal ? 1 : 0);
        }

        if ($this->timing !== null) {
            $options[] = $this->createDefElemInt('timing', $this->timing ? 1 : 0);
        }

        if ($this->summary !== null) {
            $options[] = $this->createDefElemInt('summary', $this->summary ? 1 : 0);
        }

        if ($this->memory !== null) {
            $options[] = $this->createDefElemInt('memory', $this->memory ? 1 : 0);
        }

        if ($this->format !== null) {
            $options[] = $this->createDefElemString('format', $this->format->value);
        }

        if ($options !== []) {
            $stmt->setOptions($options);
        }

        return $stmt;
    }

    public function verbose(): ExplainFinalStep
    {
        return new self(
            $this->query,
            $this->analyze,
            true,
            $this->costs,
            $this->settings,
            $this->buffers,
            $this->wal,
            $this->timing,
            $this->summary,
            $this->memory,
            $this->format,
        );
    }

    public function wal(bool $enabled = true): ExplainFinalStep
    {
        return new self(
            $this->query,
            $this->analyze,
            $this->verbose,
            $this->costs,
            $this->settings,
            $this->buffers,
            $enabled,
            $this->timing,
            $this->summary,
            $this->memory,
            $this->format,
        );
    }

    private function createDefElemBool(string $name): Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }

    private function createDefElemInt(string $name, int $value): Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $int = new Integer();
        $int->setIval($value);

        $intNode = new Node(['integer' => $int]);
        $defElem->setArg($intNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }

    private function createDefElemString(string $name, string $value): Node
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

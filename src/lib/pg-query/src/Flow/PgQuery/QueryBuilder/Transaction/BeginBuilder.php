<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Transaction;

use Flow\PgQuery\Protobuf\AST\{A_Const, DefElem, Integer, Node, PBString, TransactionStmt, TransactionStmtKind};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class BeginBuilder implements BeginOptionsStep
{
    use AstToSql;

    private function __construct(
        private ?IsolationLevel $isolationLevel = null,
        private ?bool $readOnly = null,
        private ?bool $deferrable = null,
    ) {
    }

    public static function create() : BeginOptionsStep
    {
        return new self();
    }

    public function deferrable() : BeginOptionsStep
    {
        return new self(
            $this->isolationLevel,
            $this->readOnly,
            true,
        );
    }

    public function isolationLevel(IsolationLevel $level) : BeginOptionsStep
    {
        return new self(
            $level,
            $this->readOnly,
            $this->deferrable,
        );
    }

    public function notDeferrable() : BeginOptionsStep
    {
        return new self(
            $this->isolationLevel,
            $this->readOnly,
            false,
        );
    }

    public function readOnly() : BeginOptionsStep
    {
        return new self(
            $this->isolationLevel,
            true,
            $this->deferrable,
        );
    }

    public function readWrite() : BeginOptionsStep
    {
        return new self(
            $this->isolationLevel,
            false,
            $this->deferrable,
        );
    }

    public function toAst() : TransactionStmt
    {
        $stmt = new TransactionStmt();
        $stmt->setKind(TransactionStmtKind::TRANS_STMT_BEGIN);

        $options = $this->buildOptions();

        if ($options !== []) {
            $stmt->setOptions($options);
        }

        return $stmt;
    }

    /**
     * @return list<Node>
     */
    private function buildOptions() : array
    {
        $options = [];

        if ($this->isolationLevel !== null) {
            $options[] = $this->createIsolationLevelOption($this->isolationLevel);
        }

        if ($this->readOnly !== null) {
            $options[] = $this->createBoolOption('transaction_read_only', $this->readOnly);
        }

        if ($this->deferrable !== null) {
            $options[] = $this->createBoolOption('transaction_deferrable', $this->deferrable);
        }

        return $options;
    }

    private function createBoolOption(string $name, bool $value) : Node
    {
        $defElem = new DefElem();
        $defElem->setDefname($name);

        $aConst = new A_Const();
        $ival = new Integer();
        $ival->setIval($value ? 1 : 0);
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $aConst->setIval($ival);

        $argNode = new Node();
        $argNode->setAConst($aConst);
        $defElem->setArg($argNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }

    private function createIsolationLevelOption(IsolationLevel $level) : Node
    {
        $defElem = new DefElem();
        $defElem->setDefname('transaction_isolation');

        $aConst = new A_Const();
        $sval = new PBString();
        $sval->setSval($level->value);
        $aConst->setSval($sval);

        $argNode = new Node();
        $argNode->setAConst($aConst);
        $defElem->setArg($argNode);

        $node = new Node();
        $node->setDefElem($defElem);

        return $node;
    }
}

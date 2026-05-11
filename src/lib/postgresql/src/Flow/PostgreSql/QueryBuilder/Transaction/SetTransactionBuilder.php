<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Transaction;

use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\VariableSetKind;
use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class SetTransactionBuilder implements SetTransactionOptionsStep
{
    use AstToSql;

    private function __construct(
        private bool $isSession = false,
        private ?string $snapshotId = null,
        private ?IsolationLevel $isolationLevel = null,
        private ?bool $readOnly = null,
        private ?bool $deferrable = null,
    ) {}

    public static function create(): SetTransactionOptionsStep
    {
        return new self();
    }

    public static function session(): SetTransactionOptionsStep
    {
        return new self(true);
    }

    public function deferrable(): SetTransactionOptionsStep
    {
        return new self($this->isSession, $this->snapshotId, $this->isolationLevel, $this->readOnly, true);
    }

    public function isolationLevel(IsolationLevel $level): SetTransactionOptionsStep
    {
        return new self($this->isSession, $this->snapshotId, $level, $this->readOnly, $this->deferrable);
    }

    public function notDeferrable(): SetTransactionOptionsStep
    {
        return new self($this->isSession, $this->snapshotId, $this->isolationLevel, $this->readOnly, false);
    }

    public function readOnly(): SetTransactionOptionsStep
    {
        return new self($this->isSession, $this->snapshotId, $this->isolationLevel, true, $this->deferrable);
    }

    public function readWrite(): SetTransactionOptionsStep
    {
        return new self($this->isSession, $this->snapshotId, $this->isolationLevel, false, $this->deferrable);
    }

    public function snapshot(string $snapshotId): SetTransactionFinalStep
    {
        return new self($this->isSession, $snapshotId, $this->isolationLevel, $this->readOnly, $this->deferrable);
    }

    public function toAst(): VariableSetStmt
    {
        $stmt = new VariableSetStmt();
        $stmt->setKind(VariableSetKind::VAR_SET_MULTI);

        if ($this->snapshotId !== null) {
            $stmt->setName('TRANSACTION SNAPSHOT');
            $sval = new PBString();
            $sval->setSval($this->snapshotId);
            $aConst = new A_Const();
            $aConst->setSval($sval);
            $argNode = new Node();
            $argNode->setAConst($aConst);
            $stmt->setArgs([$argNode]);
        } elseif ($this->isSession) {
            $stmt->setName('SESSION CHARACTERISTICS');
            $stmt->setArgs($this->buildOptions());
        } else {
            $stmt->setName('TRANSACTION');
            $stmt->setArgs($this->buildOptions());
        }

        return $stmt;
    }

    /**
     * @return list<Node>
     */
    private function buildOptions(): array
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

    private function createBoolOption(string $name, bool $value): Node
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

    private function createIsolationLevelOption(IsolationLevel $level): Node
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

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\{ModificationContext, NodeModifier};
use Flow\PostgreSql\Protobuf\AST\{DefElem, ExplainStmt, Integer, Node, PBString, SelectStmt};

final readonly class ExplainModifier implements NodeModifier
{
    public function __construct(
        private ExplainConfig $config,
    ) {
    }

    public static function nodeClass() : string
    {
        return SelectStmt::class;
    }

    public function modify(object $node, ModificationContext $context) : ?object
    {
        if (!$context->isTopLevel()) {
            return null;
        }

        if (!$node instanceof SelectStmt) {
            return null;
        }

        return $this->wrapWithExplain($node);
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

    private function wrapWithExplain(SelectStmt $stmt) : Node
    {
        $explainStmt = new ExplainStmt();

        $queryNode = new Node();
        $queryNode->setSelectStmt($stmt);
        $explainStmt->setQuery($queryNode);

        $options = [];

        if ($this->config->analyze) {
            $options[] = $this->createDefElemBool('analyze');
        }

        if ($this->config->verbose) {
            $options[] = $this->createDefElemBool('verbose');
        }

        $options[] = $this->createDefElemInt('costs', $this->config->costs ? 1 : 0);

        if ($this->config->buffers) {
            $options[] = $this->createDefElemInt('buffers', 1);
        }

        if ($this->config->timing) {
            $options[] = $this->createDefElemInt('timing', 1);
        }

        if ($this->config->summary) {
            $options[] = $this->createDefElemInt('summary', 1);
        }

        if ($this->config->memory) {
            $options[] = $this->createDefElemInt('memory', 1);
        }

        if ($this->config->settings) {
            $options[] = $this->createDefElemInt('settings', 1);
        }

        if ($this->config->wal) {
            $options[] = $this->createDefElemInt('wal', 1);
        }

        $options[] = $this->createDefElemString('format', $this->config->format->value);

        $explainStmt->setOptions($options);

        $resultNode = new Node();
        $resultNode->setExplainStmt($explainStmt);

        return $resultNode;
    }
}

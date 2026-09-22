<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\AST\Nodes\Exception\InvalidStatementException;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\ExplainStmt;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\PBString;

use function Flow\Types\DSL\type_instance_of;
use function in_array;
use function sprintf;

final readonly class ExplainModifier implements NodeModifier
{
    private const array EXPLAINABLE = [
        'select_stmt',
        'insert_stmt',
        'update_stmt',
        'delete_stmt',
        'merge_stmt',
        'create_table_as_stmt',
        'execute_stmt',
        'declare_cursor_stmt',
    ];

    public function __construct(
        private ExplainConfig $config,
    ) {}

    public static function nodeClasses(): array
    {
        return [ParseResult::class];
    }

    public function modify(object $node, ModificationContext $context): int
    {
        $parseResult = type_instance_of(ParseResult::class)->assert($node);

        foreach ($parseResult->getStmts() as $rawStmt) {
            $which = $rawStmt->getStmt()?->getNode() ?? '';

            if (!in_array($which, self::EXPLAINABLE, true)) {
                throw new InvalidStatementException(sprintf('EXPLAIN cannot explain a "%s" statement', $which));
            }
        }

        // validated above before any statement is wrapped, so a rejected query is left untouched
        foreach ($parseResult->getStmts() as $rawStmt) {
            $statement = $rawStmt->getStmt();

            if ($statement !== null) {
                $rawStmt->setStmt($this->wrapWithExplain($statement));
            }
        }

        return NodeModifier::DONT_TRAVERSE_CHILDREN;
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

    private function wrapWithExplain(Node $statement): Node
    {
        $explainStmt = new ExplainStmt();
        $explainStmt->setQuery($statement);

        $options = [];

        if ($this->config->analyze) {
            $options[] = $this->createDefElemBool('analyze');
        }

        if ($this->config->verbose) {
            $options[] = $this->createDefElemBool('verbose');
        }

        $options[] = $this->createDefElemInt('costs', $this->config->costs ? 1 : 0);

        if ($this->config->analyze) {
            $options[] = $this->createDefElemInt('buffers', $this->config->buffers ? 1 : 0);
            $options[] = $this->createDefElemInt('timing', $this->config->timing ? 1 : 0);
            $options[] = $this->createDefElemInt('summary', $this->config->summary ? 1 : 0);

            if ($this->config->memory) {
                $options[] = $this->createDefElemInt('memory', 1);
            }

            if ($this->config->settings) {
                $options[] = $this->createDefElemInt('settings', 1);
            }

            if ($this->config->wal) {
                $options[] = $this->createDefElemInt('wal', 1);
            }
        }

        $options[] = $this->createDefElemString('format', $this->config->format->value);

        $explainStmt->setOptions($options);

        $resultNode = new Node();
        $resultNode->setExplainStmt($explainStmt);

        return $resultNode;
    }
}

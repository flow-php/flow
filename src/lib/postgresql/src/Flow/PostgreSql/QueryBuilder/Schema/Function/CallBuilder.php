<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\CallStmt;
use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBFloat;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class CallBuilder implements CallFinalStep
{
    use AstToSql;

    /**
     * @param list<Node> $arguments
     */
    private function __construct(
        private string $procedure,
        private array $arguments = [],
    ) {}

    public static function create(string $procedure): CallFinalStep
    {
        return new self($procedure);
    }

    public function toAst(): CallStmt
    {
        $stmt = new CallStmt();

        $funcCall = new FuncCall();

        $funcnameNodes = [];
        $str = new PBString();
        $str->setSval($this->procedure);
        $node = new Node();
        $node->setString($str);
        $funcnameNodes[] = $node;
        $funcCall->setFuncname($funcnameNodes);

        if ($this->arguments !== []) {
            $funcCall->setArgs($this->arguments);
        }

        $stmt->setFunccall($funcCall);

        return $stmt;
    }

    public function with(mixed ...$args): CallFinalStep
    {
        $argNodes = [];

        foreach ($args as $arg) {
            if ($arg instanceof Node) {
                $argNodes[] = $arg;
            } elseif (\is_int($arg)) {
                $argNodes[] = $this->createIntegerNode($arg);
            } elseif (\is_string($arg)) {
                $argNodes[] = $this->createStringNode($arg);
            } elseif (\is_bool($arg)) {
                $argNodes[] = $this->createBoolNode($arg);
            } elseif (\is_float($arg)) {
                $argNodes[] = $this->createFloatNode($arg);
            } elseif ($arg === null) {
                $argNodes[] = $this->createNullNode();
            }
        }

        return new self($this->procedure, \array_merge($this->arguments, $argNodes));
    }

    private function createBoolNode(bool $value): Node
    {
        $boolean = new Boolean();
        $boolean->setBoolval($value);

        $aConst = new A_Const();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says bool but actually expects Boolean) */
        $aConst->setBoolval($boolean);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    private function createFloatNode(float $value): Node
    {
        $float = new PBFloat();
        $float->setFval((string) $value);

        $aConst = new A_Const();
        $aConst->setFval($float);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    private function createIntegerNode(int $value): Node
    {
        $integer = new Integer();
        $integer->setIval($value);

        $aConst = new A_Const();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $aConst->setIval($integer);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    private function createNullNode(): Node
    {
        $aConst = new A_Const();
        $aConst->setIsnull(true);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    private function createStringNode(string $value): Node
    {
        $str = new PBString();
        $str->setSval($value);

        $aConst = new A_Const();
        $aConst->setSval($str);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }
}

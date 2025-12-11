<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Extension;

use Flow\PgQuery\Protobuf\AST\{CreateExtensionStmt, DefElem, Integer, Node, PBString};
use Flow\PgQuery\QueryBuilder\AstToSql;

final readonly class CreateExtensionBuilder implements CreateExtensionOptionsStep
{
    use AstToSql;

    /**
     * @param list<array{name: string, arg: ?Node}> $options
     */
    private function __construct(
        private string $name,
        private bool $ifNotExists = false,
        private array $options = [],
    ) {
    }

    public static function create(string $name) : CreateExtensionOptionsStep
    {
        return new self($name);
    }

    public function cascade() : CreateExtensionOptionsStep
    {
        return $this->withBooleanOption('cascade', true);
    }

    public function ifNotExists() : CreateExtensionOptionsStep
    {
        return new self(
            $this->name,
            true,
            $this->options,
        );
    }

    public function schema(string $schema) : CreateExtensionOptionsStep
    {
        return $this->withStringOption('schema', $schema);
    }

    public function toAst() : CreateExtensionStmt
    {
        $stmt = new CreateExtensionStmt();
        $stmt->setExtname($this->name);
        $stmt->setIfNotExists($this->ifNotExists);

        $optionNodes = [];

        foreach ($this->options as $option) {
            $defElem = new DefElem();
            $defElem->setDefname($option['name']);

            if ($option['arg'] !== null) {
                $defElem->setArg($option['arg']);
            }

            $node = new Node();
            $node->setDefElem($defElem);
            $optionNodes[] = $node;
        }

        if ($optionNodes !== []) {
            $stmt->setOptions($optionNodes);
        }

        return $stmt;
    }

    public function version(string $version) : CreateExtensionOptionsStep
    {
        return $this->withStringOption('new_version', $version);
    }

    private function withBooleanOption(string $name, bool $value) : self
    {
        $integer = new Integer();
        $integer->setIval($value ? 1 : 0);

        $argNode = new Node();
        /** @phpstan-ignore argument.type (protobuf PHPDoc says int but actually expects Integer) */
        $argNode->setInteger($integer);

        return $this->withOption($name, $argNode);
    }

    private function withOption(string $name, ?Node $arg) : self
    {
        $newOptions = $this->options;
        $newOptions[] = ['name' => $name, 'arg' => $arg];

        return new self(
            $this->name,
            $this->ifNotExists,
            $newOptions,
        );
    }

    private function withStringOption(string $name, string $value) : self
    {
        $str = new PBString();
        $str->setSval($value);

        $argNode = new Node();
        $argNode->setString($str);

        return $this->withOption($name, $argNode);
    }
}

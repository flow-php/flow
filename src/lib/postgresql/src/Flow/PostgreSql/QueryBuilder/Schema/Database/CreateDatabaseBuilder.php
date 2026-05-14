<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Database;

use Flow\PostgreSql\Protobuf\AST\CreatedbStmt;
use Flow\PostgreSql\Protobuf\AST\DefElem;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class CreateDatabaseBuilder implements CreateDatabaseOptionsStep
{
    use AstToSql;

    /**
     * @param list<array{name: string, arg: ?Node}> $options
     */
    private function __construct(
        private string $name,
        private array $options = [],
    ) {}

    public static function create(string $name): CreateDatabaseOptionsStep
    {
        return new self($name);
    }

    public function allowConnections(bool $allow): self
    {
        return $this->withOption('allow_connections', $this->booleanNode($allow));
    }

    public function builtinLocale(string $locale): self
    {
        return $this->withOption('builtin_locale', $this->stringNode($locale));
    }

    public function collationVersion(string $version): self
    {
        return $this->withOption('collation_version', $this->stringNode($version));
    }

    public function connectionLimit(int $limit): self
    {
        return $this->withOption('connection_limit', $this->integerNode($limit));
    }

    public function encoding(string $encoding): self
    {
        return $this->withOption('encoding', $this->stringNode($encoding));
    }

    public function icuLocale(string $locale): self
    {
        return $this->withOption('icu_locale', $this->stringNode($locale));
    }

    public function icuRules(string $rules): self
    {
        return $this->withOption('icu_rules', $this->stringNode($rules));
    }

    public function ifNotExists(): self
    {
        return $this->withOption('if_not_exists', $this->booleanNode(true));
    }

    public function isTemplate(bool $template): self
    {
        return $this->withOption('is_template', $this->booleanNode($template));
    }

    public function lcCollate(string $collate): self
    {
        return $this->withOption('lc_collate', $this->stringNode($collate));
    }

    public function lcCtype(string $ctype): self
    {
        return $this->withOption('lc_ctype', $this->stringNode($ctype));
    }

    public function locale(string $locale): self
    {
        return $this->withOption('locale', $this->stringNode($locale));
    }

    public function localeProvider(string $provider): self
    {
        return $this->withOption('locale_provider', $this->stringNode($provider));
    }

    public function oid(int $oid): self
    {
        return $this->withOption('oid', $this->integerNode($oid));
    }

    public function owner(string $role): self
    {
        return $this->withOption('owner', $this->stringNode($role));
    }

    public function strategy(string $strategy): self
    {
        return $this->withOption('strategy', $this->stringNode($strategy));
    }

    public function tablespace(string $name): self
    {
        return $this->withOption('tablespace', $this->stringNode($name));
    }

    public function template(string $name): self
    {
        return $this->withOption('template', $this->stringNode($name));
    }

    public function toAst(): CreatedbStmt
    {
        $stmt = new CreatedbStmt();
        $stmt->setDbname($this->name);

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

    private function booleanNode(bool $value): Node
    {
        $integer = new Integer();
        $integer->setIval($value ? 1 : 0);

        return new Node(['integer' => $integer]);
    }

    private function integerNode(int $value): Node
    {
        $integer = new Integer();
        $integer->setIval($value);

        return new Node(['integer' => $integer]);
    }

    private function stringNode(string $value): Node
    {
        $str = new PBString();
        $str->setSval($value);

        $node = new Node();
        $node->setString($str);

        return $node;
    }

    private function withOption(string $name, ?Node $arg): self
    {
        $newOptions = $this->options;
        $newOptions[] = ['name' => $name, 'arg' => $arg];

        return new self($this->name, $newOptions);
    }
}

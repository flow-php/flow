<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\Protobuf\AST\ReturningClause as ReturningClauseNode;
use Flow\PostgreSql\Protobuf\AST\ReturningOption;
use Flow\PostgreSql\Protobuf\AST\ReturningOptionKind;
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidBuilderStateException;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use InvalidArgumentException;

use function array_values;

final readonly class ReturningClause implements AstConvertible
{
    /**
     * @param list<Expression> $expressions
     */
    public function __construct(
        private array $expressions = [],
        private ?string $oldAlias = null,
        private ?string $newAlias = null,
    ) {}

    public static function fromAst(Node $node): static
    {
        $returningClause = $node->getReturningClause();

        if ($returningClause === null) {
            throw InvalidAstException::unexpectedNodeType('ReturningClause', 'unknown');
        }

        $expressions = [];

        foreach ($returningClause->getExprs() as $expressionNode) {
            $expressions[] = ExpressionFactory::fromAst($expressionNode);
        }

        $oldAlias = null;
        $newAlias = null;

        foreach ($returningClause->getOptions() as $optionNode) {
            $option = $optionNode->getReturningOption();

            if ($option === null) {
                throw InvalidAstException::invalidFieldValue(
                    'options',
                    'ReturningClause',
                    'Expected ReturningOption node',
                );
            }

            if ($option->getOption() === ReturningOptionKind::RETURNING_OPTION_OLD) {
                $oldAlias = $option->getValue();
            } elseif ($option->getOption() === ReturningOptionKind::RETURNING_OPTION_NEW) {
                $newAlias = $option->getValue();
            }
        }

        return new self($expressions, $oldAlias, $newAlias);
    }

    public function aliases(?string $old, ?string $new): self
    {
        if ($old === null && $new === null) {
            throw new InvalidArgumentException('returningAliases() needs an old or a new alias');
        }

        return new self($this->expressions, $old, $new);
    }

    /**
     * @return list<Expression>
     */
    public function expressions(): array
    {
        return $this->expressions;
    }

    public function returning(Expression ...$expressions): self
    {
        if ($expressions === []) {
            throw new InvalidArgumentException(
                'returning() needs at least one expression. Call returningAll() for RETURNING *.',
            );
        }

        return new self(array_values($expressions), $this->oldAlias, $this->newAlias);
    }

    public function toAst(): Node
    {
        $options = [];

        if ($this->oldAlias !== null) {
            $options[] = self::optionNode(ReturningOptionKind::RETURNING_OPTION_OLD, $this->oldAlias);
        }

        if ($this->newAlias !== null) {
            $options[] = self::optionNode(ReturningOptionKind::RETURNING_OPTION_NEW, $this->newAlias);
        }

        if ($this->expressions === [] && $options !== []) {
            throw InvalidBuilderStateException::returningAliasesWithoutList($this->oldAlias, $this->newAlias);
        }

        $expressionNodes = [];

        foreach ($this->expressions as $expression) {
            $expressionNode = $expression->toAst();

            if ($expressionNode->getResTarget() === null) {
                $expressionNode = (new Node())->setResTarget((new ResTarget())->setVal($expressionNode));
            }

            $expressionNodes[] = $expressionNode;
        }

        $returningClause = new ReturningClauseNode();
        $returningClause->setExprs($expressionNodes);
        $returningClause->setOptions($options);

        return (new Node())->setReturningClause($returningClause);
    }

    private static function optionNode(int $kind, string $alias): Node
    {
        return (new Node())->setReturningOption(
            (new ReturningOption())
                ->setOption($kind)
                ->setValue($alias),
        );
    }
}

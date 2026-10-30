<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Join;

use Flow\ETL\Foundation\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Kernels\HashJoin\RowMerger;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Schema;

use function array_keys;
use function array_map;

final readonly class JoinSchema
{
    /**
     * @param list<string> $dropLeft left columns left out of the output (a duplicated join column)
     * @param list<string> $dropRight right columns left out of the output (a duplicated join column)
     */
    public function __construct(
        private string $prefix = '',
        private array $dropLeft = [],
        private array $dropRight = [],
        private JoinType $type = JoinType::inner,
    ) {}

    public function cross(Schema $left, Schema $right): Schema
    {
        return (new self($this->prefix, $this->dropLeft, $this->dropRight))->output($left, $right);
    }

    public function merger(Schema $left, Schema $right): RowMerger
    {
        return new RowMerger(
            $this->output($left, $right),
            array_map('strval', array_keys($left->gracefulRemove(...$this->dropLeft)->definitions())),
            $this->type === JoinType::left_anti
                ? []
                : array_map('strval', array_keys($right->gracefulRemove(...$this->dropRight)->definitions())),
        );
    }

    public function output(Schema $left, Schema $right): Schema
    {
        $leftSide = $left->gracefulRemove(...$this->dropLeft);
        $rightSide = $right->gracefulRemove(...$this->dropRight);

        if ($this->prefix !== '') {
            foreach ($rightSide->names() as $name) {
                $rightSide = $rightSide->rename($name, $this->prefix . $name);
            }
        }

        try {
            return match ($this->type) {
                JoinType::left_anti => $leftSide,
                JoinType::left => $leftSide->add(...$rightSide->makeNullable()->definitions()),
                JoinType::right => $leftSide->makeNullable()->add(...$rightSide->definitions()),
                JoinType::inner => $leftSide->add(...$rightSide->definitions()),
            };
        } catch (SchemaDefinitionNotUniqueException $e) {
            throw new SchemaDefinitionNotUniqueException(
                $e->getMessage() . '. Please consider using join prefix option',
                (int) $e->getCode(),
                $e,
            );
        }
    }
}

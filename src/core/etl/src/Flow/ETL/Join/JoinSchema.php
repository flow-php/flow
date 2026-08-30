<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Schema;

final readonly class JoinSchema
{
    /**
     * @param array<string> $dropLeft
     * @param array<string> $dropRight
     */
    public function __construct(
        private string $prefix = '',
        private array $dropLeft = [],
        private array $dropRight = [],
    ) {}

    public function cross(Schema $left, Schema $right): Schema
    {
        return $this->of(Join::inner, $left, $right);
    }

    public function of(Join $type, Schema $left, Schema $right): Schema
    {
        $leftSide = $left->gracefulRemove(...$this->dropLeft);
        $rightSide = $right->gracefulRemove(...$this->dropRight);

        if ($this->prefix !== '') {
            foreach ($rightSide->references() as $ref) {
                $rightSide = $rightSide->rename($ref, $this->prefix . $ref->name());
            }
        }

        return match ($type) {
            Join::left_anti => $leftSide,
            Join::left => $leftSide->add(...$rightSide->makeNullable()->definitions()),
            Join::right => $leftSide->makeNullable()->add(...$rightSide->definitions()),
            Join::inner => $leftSide->add(...$rightSide->definitions()),
        };
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Join\HashJoin\RowMerger;

/**
 * @type Drops = list<string>
 */
final readonly class JoinShape
{
    /**
     * @param Drops $dropLeft
     * @param Drops $dropRight
     */
    private function __construct(
        private string $prefix,
        private array $dropLeft,
        private array $dropRight,
    ) {}

    public static function of(Expression $expression, Join $type): self
    {
        $duplicates = [];

        if ($expression->prefix() === '') {
            foreach ($expression->left() as $leftRef) {
                foreach ($expression->right() as $rightRef) {
                    if ($leftRef->name() === $rightRef->name()) {
                        $duplicates[] = $leftRef->name();

                        continue 2;
                    }
                }
            }
        }

        return new self(
            $expression->prefix(),
            $type === Join::right ? $duplicates : [],
            $type === Join::right ? [] : $duplicates,
        );
    }

    public function merger(): RowMerger
    {
        return new RowMerger($this->prefix, $this->dropLeft, $this->dropRight);
    }

    public function schema(): JoinSchema
    {
        return new JoinSchema($this->prefix, $this->dropLeft, $this->dropRight);
    }
}

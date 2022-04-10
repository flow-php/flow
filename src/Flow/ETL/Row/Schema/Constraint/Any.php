<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Constraint;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Constraint;

/**
 * @implements Constraint<array{constraints: array<Constraint>}>
 */
final class Any implements Constraint
{
    /**
     * @var array<Constraint>
     */
    private readonly array $constraints;

    public function __construct(Constraint ...$constraints)
    {
        $this->constraints = $constraints;
    }

    // @codeCoverageIgnoreStart
    public function __serialize() : array
    {
        return [
            'constraints' => $this->constraints,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->constraints = $data['constraints'];
    }
    // @codeCoverageIgnoreEnd

    public function isSatisfiedBy(Entry $entry) : bool
    {
        foreach ($this->constraints as $constraint) {
            if ($constraint->isSatisfiedBy($entry)) {
                return true;
            }
        }

        return false;
    }
}

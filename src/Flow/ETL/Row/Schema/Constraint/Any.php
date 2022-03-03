<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema\Constraint;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Constraint;

final class Any implements Constraint
{
    /**
     * @var array<Constraint>
     */
    private array $constraints;

    public function __construct(Constraint ...$constraints)
    {
        $this->constraints = $constraints;
    }

    // @codeCoverageIgnoreStart
    /**
     * @return array{constraints: array<Constraint>}
     */
    public function __serialize() : array
    {
        return [
            'constraints' => $this->constraints,
        ];
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{constraints: array<Constraint>} $data
     */
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

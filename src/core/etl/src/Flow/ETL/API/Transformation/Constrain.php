<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Operators\Constraint;
use NoDiscard;

use function array_values;

final readonly class Constrain implements Transformation
{
    /**
     * @var list<Constraint>
     */
    private array $constraints;

    public function __construct(
        private Constraint $constraint,
        Constraint ...$constraints,
    ) {
        $this->constraints = array_values($constraints);
    }

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->constrain($this->constraint, ...$this->constraints);
    }
}

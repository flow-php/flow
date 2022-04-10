<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Condition\ValidValue;

use Flow\ETL\Exception\RuntimeException;
use Symfony\Component\Validator\Constraint;
use Symfony\Component\Validator\Validation;
use Symfony\Component\Validator\Validator\ValidatorInterface;

if (!\class_exists(\Symfony\Component\Validator\Validation::class)) {
    throw new RuntimeException("Symfony\Component\Validator\Validation class not found, please add symfony/validator dependency to the project first.");
}

final class SymfonyValidator implements Validator
{
    private readonly ValidatorInterface $validator;

    /**
     * @param array<Constraint> $constraints
     * @param null|ValidatorInterface $validator
     */
    public function __construct(private readonly array $constraints = [], ValidatorInterface $validator = null)
    {
        $this->validator = $validator ?: Validation::createValidator();
    }

    public function isValid(mixed $value) : bool
    {
        return $this->validator->validate($value, $this->constraints)->count() === 0;
    }
}

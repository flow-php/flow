<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter\Filter\ValidValue;

use Flow\ETL\Exception\RuntimeException;
use Symfony\Component\Validator\Constraint;
use Symfony\Component\Validator\Validation;
use Symfony\Component\Validator\Validator\ValidatorInterface;

if (!\class_exists(\Symfony\Component\Validator\Validation::class)) {
    throw new RuntimeException("Symfony\Component\Validator\Validation class not found, please add symfony/validator dependency to the project first.");
}

/**
 * @implements Validator<array{validator: ValidatorInterface, constraints: array<Constraint>}>
 */
final class SymfonyValidator implements Validator
{
    private ValidatorInterface $validator;

    /**
     * @param array<Constraint> $constraints
     * @param null|ValidatorInterface $validator
     */
    public function __construct(private array $constraints = [], ValidatorInterface $validator = null)
    {
        $this->validator = $validator ?: Validation::createValidator();
    }

    public function __serialize() : array
    {
        return [
            'constraints' => $this->constraints,
            'validator' => $this->validator,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->validator = $data['validator'];
        $this->constraints = $data['constraints'];
    }

    public function isValid($value) : bool
    {
        return $this->validator->validate($value, $this->constraints)->count() === 0;
    }
}

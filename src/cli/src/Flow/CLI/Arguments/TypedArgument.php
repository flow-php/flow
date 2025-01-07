<?php

declare(strict_types=1);

namespace Flow\CLI\Arguments;

use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

final readonly class TypedArgument
{
    public function __construct(private string $name)
    {
    }

    public function asInt(InputInterface $input) : int
    {
        $option = $this->asIntNullable($input);

        if ($option === null) {
            throw new InvalidArgumentException("Argument '{$this->name}' is required.");
        }

        return $option;
    }

    public function asIntNullable(InputInterface $input) : ?int
    {
        $option = $input->getArgument($this->name);

        if ($option === null) {
            return null;
        }

        if (!\is_int($option)) {
            throw new InvalidArgumentException("Argument '{$this->name}' must be an integer.");
        }

        return $option;
    }

    public function asString(InputInterface $input) : string
    {
        $option = $this->asStringNullable($input);

        if ($option === null) {
            throw new InvalidArgumentException("Argument '{$this->name}' is required.");
        }

        return $option;
    }

    public function asStringNullable(InputInterface $input) : ?string
    {
        $option = $input->getArgument($this->name);

        if ($option === null) {
            return null;
        }

        if (!\is_string($option)) {
            throw new InvalidArgumentException("Argument '{$this->name}' must be a string.");
        }

        return $option;
    }
}

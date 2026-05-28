<?php

declare(strict_types=1);

namespace Flow\CLI\Arguments;

use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

use function Flow\Types\DSL\type_string;
use function is_numeric;

final readonly class TypedArgument
{
    public function __construct(
        private string $name,
    ) {}

    public function asInt(InputInterface $input): int
    {
        $option = $this->asIntNullable($input);

        if ($option === null) {
            throw new InvalidArgumentException("Argument '{$this->name}' is required.");
        }

        return $option;
    }

    public function asIntNullable(InputInterface $input): ?int
    {
        if ($input->getArgument($this->name) === null) {
            return null;
        }

        if (!is_numeric($input->getArgument($this->name))) {
            throw new InvalidArgumentException("Argument '{$this->name}' must be an integer.");
        }

        return (int) $input->getArgument($this->name);
    }

    public function asString(InputInterface $input): string
    {
        $option = $this->asStringNullable($input);

        if ($option === null) {
            throw new InvalidArgumentException("Argument '{$this->name}' is required.");
        }

        return $option;
    }

    public function asStringNullable(InputInterface $input): ?string
    {
        if ($input->getArgument($this->name) === null) {
            return null;
        }

        return type_string()->assert($input->getArgument($this->name));
    }
}

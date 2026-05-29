<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Stringable;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

use function count;
use function filter_var;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function is_int;
use function is_numeric;
use function is_scalar;
use function is_string;

final readonly class TypedOption
{
    public function __construct(
        private string $name,
    ) {}

    public function asBool(InputInterface $input): bool
    {
        return $input->getOption($this->name) !== false;
    }

    public function asBoolNullable(InputInterface $input): ?bool
    {
        if ($input->getOption($this->name) === null) {
            return null;
        }

        if (is_string($input->getOption($this->name))) {
            return filter_var($input->getOption($this->name), FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE);
        }

        if (is_int($input->getOption($this->name))) {
            return type_integer()->assert($input->getOption($this->name)) !== 0;
        }

        return type_boolean()->assert($input->getOption($this->name));
    }

    public function asInt(InputInterface $input, ?int $default = null): int
    {
        $option = $this->asIntNullable($input);

        if ($option === null && $default === null) {
            throw new InvalidArgumentException("Option '{$this->name}' is required.");
        }

        return $option ?? $default;
    }

    public function asIntNullable(InputInterface $input): ?int
    {
        if ($input->getOption($this->name) === null) {
            return null;
        }

        if (!is_numeric($input->getOption($this->name))) {
            throw new InvalidArgumentException("Option '{$this->name}' must be an integer.");
        }

        return (int) $input->getOption($this->name);
    }

    /**
     * @return array<array-key, string>
     */
    public function asListOfStrings(InputInterface $input): array
    {
        $option = $this->asListOfStringsNullable($input);

        return $option ?? [];
    }

    /**
     * @return null|array<array-key, string>
     */
    public function asListOfStringsNullable(InputInterface $input): ?array
    {
        if ($input->getOption($this->name) === null) {
            return null;
        }

        $option = type_array()->assert($input->getOption($this->name));

        if (!count($option)) {
            return null;
        }

        return array_map(static fn(mixed $value): string => is_scalar($value) || $value instanceof Stringable
            ? (string) $value
            : '', $option);
    }

    public function asString(InputInterface $input, ?string $default = null): string
    {
        $option = $this->asStringNullable($input);

        if ($option === null && $default === null) {
            throw new InvalidArgumentException("Option '{$this->name}' is required.");
        }

        return $option ?? $default;
    }

    public function asStringNullable(InputInterface $input): ?string
    {
        if ($input->getOption($this->name) === null) {
            return null;
        }

        return type_string()->assert($input->getOption($this->name));
    }
}

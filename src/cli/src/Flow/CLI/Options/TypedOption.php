<?php

declare(strict_types=1);

namespace Flow\CLI\Options;

use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Input\InputInterface;

final readonly class TypedOption
{
    public function __construct(private string $name)
    {
    }

    public function asBool(InputInterface $input) : bool
    {
        return $input->getOption($this->name) !== false;
    }

    public function asBoolNullable(InputInterface $input) : ?bool
    {
        $option = $input->getOption($this->name);

        if ($option === null) {
            return null;
        }

        if (\is_string($option)) {
            $option = \filter_var($option, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE);

            return $option ?? null;
        }

        if (\is_int($option)) {
            return (bool) $option;
        }

        if (!\is_bool($option)) {
            throw new InvalidArgumentException("Option '{$this->name}' must be a boolean.");
        }

        return $option;
    }

    public function asInt(InputInterface $input, ?int $default = null) : int
    {
        $option = $this->asIntNullable($input);

        if ($option === null && $default === null) {
            throw new InvalidArgumentException("Option '{$this->name}' is required.");
        }

        return $option ?? $default;
    }

    public function asIntNullable(InputInterface $input) : ?int
    {
        $option = $input->getOption($this->name);

        if ($option === null) {
            return null;
        }

        if (!\is_numeric($option)) {
            throw new InvalidArgumentException("Option '{$this->name}' must be an integer.");
        }

        return (int) $option;
    }

    public function asListOfStrings(InputInterface $input) : array
    {
        $option = $this->asListOfStringsNullable($input);

        return $option ?? [];
    }

    public function asListOfStringsNullable(InputInterface $input) : ?array
    {
        $option = $input->getOption($this->name);

        if ($option === null) {
            return null;
        }

        if (!\is_array($option)) {
            throw new InvalidArgumentException("Option '{$this->name}' must be an array.");
        }

        if (!\count($option)) {
            return null;
        }

        /**
         * @var array<string> $options
         */
        $options = [];

        foreach ($option as $value) {
            $options[] = (string) $value;
        }

        return $options;
    }

    public function asString(InputInterface $input, ?string $default = null) : string
    {
        $option = $this->asStringNullable($input);

        if ($option === null && $default === null) {
            throw new InvalidArgumentException("Option '{$this->name}' is required.");
        }

        return $option ?? $default;
    }

    public function asStringNullable(InputInterface $input) : ?string
    {
        $option = $input->getOption($this->name);

        if ($option === null) {
            return null;
        }

        if (!\is_string($option)) {
            throw new InvalidArgumentException("Option '{$this->name}' must be a string.");
        }

        return $option;
    }
}

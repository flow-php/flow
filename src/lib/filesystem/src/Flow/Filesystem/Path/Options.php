<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path;

use function Flow\Types\DSL\type_string;
use Symfony\Component\OptionsResolver\Exception\MissingOptionsException;

final class Options
{
    /**
     * @var array<string, string>
     */
    private array $options;

    /**
     * @param array<array-key, mixed> $options
     */
    public function __construct(array $options)
    {
        $normalizedOptions = [];

        foreach ($options as $option => $value) {
            $normalizedOptions[\mb_strtolower(type_string()->cast($option))] = type_string()->cast($value);
        }

        $this->options = $normalizedOptions;
    }

    public function assertHas(string $option) : void
    {
        if (!$this->has($option)) {
            throw new MissingOptionsException("Option '{$option}' is missing in Path object.");
        }
    }

    public function getAsString(string $option, ?string $default = null) : ?string
    {
        if ($this->has($option)) {
            return (string) $this->options[$option];
        }

        return $default;
    }

    public function has(string $option) : bool
    {
        return isset($this->options[\mb_strtolower($option)]);
    }

    /**
     * @return array<string, string>
     */
    public function toArray() : array
    {
        return $this->options;
    }
}

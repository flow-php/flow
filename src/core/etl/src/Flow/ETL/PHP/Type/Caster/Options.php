<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Caster;

use Flow\ETL\PHP\Type\Caster\FloatCastingHandler\RoundingMode;

final class Options
{
    public const FLOAT_ROUNDING_MODE = 'casting_mode';

    /**
     * @var array<string, mixed>
     */
    private array $options;

    public function __construct()
    {
        $this->options = [
            self::FLOAT_ROUNDING_MODE => RoundingMode::ROUND_HALF_UP,
        ];
    }

    public function get(string $key) : mixed
    {
        if (!array_key_exists($key, $this->options)) {
            throw new \InvalidArgumentException("Option with key: {$key} does not exist, available options: " . implode(', ', array_keys($this->options)));
        }

        return $this->options[$key];
    }

    public function set(string $key, mixed $value) : void
    {
        if (!array_key_exists($key, $this->options)) {
            throw new \InvalidArgumentException("Option with key: {$key} does not exist, available options: " . implode(', ', array_keys($this->options)));
        }

        $this->options[$key] = $value;
    }
}

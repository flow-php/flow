<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;

final readonly class FunctionArgument
{
    private function __construct(
        public ColumnType $type,
        public ?string $name = null,
        public ArgumentMode $mode = ArgumentMode::IN,
        public ?string $default = null,
    ) {}

    public static function of(ColumnType $type): self
    {
        return new self($type);
    }

    public function default(string $value): self
    {
        return new self($this->type, $this->name, $this->mode, $value);
    }

    public function in(): self
    {
        return new self($this->type, $this->name, ArgumentMode::IN, $this->default);
    }

    public function inout(): self
    {
        return new self($this->type, $this->name, ArgumentMode::INOUT, $this->default);
    }

    public function named(string $name): self
    {
        return new self($this->type, $name, $this->mode, $this->default);
    }

    public function out(): self
    {
        return new self($this->type, $this->name, ArgumentMode::OUT, $this->default);
    }

    public function variadic(): self
    {
        return new self($this->type, $this->name, ArgumentMode::VARIADIC, $this->default);
    }
}

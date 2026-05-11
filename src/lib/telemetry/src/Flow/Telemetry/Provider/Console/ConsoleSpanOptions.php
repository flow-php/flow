<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Console;

final readonly class ConsoleSpanOptions
{
    public function __construct(
        public bool $showResourceAttributes = true,
        public bool $showInstrumentationScope = true,
        public bool $showLinks = true,
        public bool $showStatusDescription = true,
        public bool $showDroppedCounts = false,
    ) {}

    public static function default(): self
    {
        return new self();
    }

    public static function minimal(): self
    {
        return new self(
            showResourceAttributes: false,
            showInstrumentationScope: false,
            showLinks: false,
            showStatusDescription: false,
            showDroppedCounts: false,
        );
    }

    public function withDroppedCounts(bool $show = true): self
    {
        return new self(
            $this->showResourceAttributes,
            $this->showInstrumentationScope,
            $this->showLinks,
            $this->showStatusDescription,
            $show,
        );
    }

    public function withInstrumentationScope(bool $show = true): self
    {
        return new self(
            $this->showResourceAttributes,
            $show,
            $this->showLinks,
            $this->showStatusDescription,
            $this->showDroppedCounts,
        );
    }

    public function withLinks(bool $show = true): self
    {
        return new self(
            $this->showResourceAttributes,
            $this->showInstrumentationScope,
            $show,
            $this->showStatusDescription,
            $this->showDroppedCounts,
        );
    }

    public function withResourceAttributes(bool $show = true): self
    {
        return new self(
            $show,
            $this->showInstrumentationScope,
            $this->showLinks,
            $this->showStatusDescription,
            $this->showDroppedCounts,
        );
    }

    public function withStatusDescription(bool $show = true): self
    {
        return new self(
            $this->showResourceAttributes,
            $this->showInstrumentationScope,
            $this->showLinks,
            $show,
            $this->showDroppedCounts,
        );
    }
}

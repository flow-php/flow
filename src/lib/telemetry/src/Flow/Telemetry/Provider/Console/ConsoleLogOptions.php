<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Console;

final readonly class ConsoleLogOptions
{
    public function __construct(
        public bool $showResourceAttributes = true,
        public bool $showInstrumentationScope = true,
        public bool $showObservedTimestamp = false,
        public bool $showDroppedAttributeCount = false,
    ) {
    }

    public static function default() : self
    {
        return new self();
    }

    public static function minimal() : self
    {
        return new self(
            showResourceAttributes: false,
            showInstrumentationScope: false,
            showObservedTimestamp: false,
            showDroppedAttributeCount: false,
        );
    }

    public function withDroppedAttributeCount(bool $show = true) : self
    {
        return new self($this->showResourceAttributes, $this->showInstrumentationScope, $this->showObservedTimestamp, $show);
    }

    public function withInstrumentationScope(bool $show = true) : self
    {
        return new self($this->showResourceAttributes, $show, $this->showObservedTimestamp, $this->showDroppedAttributeCount);
    }

    public function withObservedTimestamp(bool $show = true) : self
    {
        return new self($this->showResourceAttributes, $this->showInstrumentationScope, $show, $this->showDroppedAttributeCount);
    }

    public function withResourceAttributes(bool $show = true) : self
    {
        return new self($show, $this->showInstrumentationScope, $this->showObservedTimestamp, $this->showDroppedAttributeCount);
    }
}

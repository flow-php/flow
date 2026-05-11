<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Console;

final readonly class ConsoleMetricOptions
{
    public function __construct(
        public bool $showResourceAttributes = true,
        public bool $showInstrumentationScope = true,
        public bool $showDescription = true,
        public bool $showAggregationTemporality = false,
        public bool $showStartTimestamp = false,
        public bool $showAllExemplars = false,
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
            showDescription: false,
            showAggregationTemporality: false,
            showStartTimestamp: false,
            showAllExemplars: false,
        );
    }

    public function withAggregationTemporality(bool $show = true): self
    {
        return new self(
            $this->showResourceAttributes,
            $this->showInstrumentationScope,
            $this->showDescription,
            $show,
            $this->showStartTimestamp,
            $this->showAllExemplars,
        );
    }

    public function withAllExemplars(bool $show = true): self
    {
        return new self(
            $this->showResourceAttributes,
            $this->showInstrumentationScope,
            $this->showDescription,
            $this->showAggregationTemporality,
            $this->showStartTimestamp,
            $show,
        );
    }

    public function withDescription(bool $show = true): self
    {
        return new self(
            $this->showResourceAttributes,
            $this->showInstrumentationScope,
            $show,
            $this->showAggregationTemporality,
            $this->showStartTimestamp,
            $this->showAllExemplars,
        );
    }

    public function withInstrumentationScope(bool $show = true): self
    {
        return new self(
            $this->showResourceAttributes,
            $show,
            $this->showDescription,
            $this->showAggregationTemporality,
            $this->showStartTimestamp,
            $this->showAllExemplars,
        );
    }

    public function withResourceAttributes(bool $show = true): self
    {
        return new self(
            $show,
            $this->showInstrumentationScope,
            $this->showDescription,
            $this->showAggregationTemporality,
            $this->showStartTimestamp,
            $this->showAllExemplars,
        );
    }

    public function withStartTimestamp(bool $show = true): self
    {
        return new self(
            $this->showResourceAttributes,
            $this->showInstrumentationScope,
            $this->showDescription,
            $this->showAggregationTemporality,
            $show,
            $this->showAllExemplars,
        );
    }
}

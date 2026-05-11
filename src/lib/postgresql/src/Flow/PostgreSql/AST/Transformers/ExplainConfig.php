<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

use Flow\PostgreSql\Exception\InvalidExplainConfigException;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;

final readonly class ExplainConfig
{
    public function __construct(
        public bool $analyze = true,
        public bool $verbose = false,
        public bool $costs = true,
        public bool $buffers = true,
        public bool $timing = true,
        public bool $summary = true,
        public bool $memory = false,
        public bool $settings = false,
        public bool $wal = false,
        public ExplainFormat $format = ExplainFormat::JSON,
    ) {
        $this->validate();
    }

    public static function forAnalysis(): self
    {
        return new self(
            analyze: true,
            verbose: false,
            costs: true,
            buffers: true,
            timing: true,
            summary: true,
            memory: false,
            settings: false,
            wal: false,
            format: ExplainFormat::JSON,
        );
    }

    public static function forEstimate(): self
    {
        return new self(
            analyze: false,
            verbose: false,
            costs: true,
            buffers: false,
            timing: false,
            summary: false,
            memory: false,
            settings: false,
            wal: false,
            format: ExplainFormat::JSON,
        );
    }

    /**
     * @param array{
     *     analyze: bool,
     *     verbose: bool,
     *     costs: bool,
     *     buffers: bool,
     *     timing: bool,
     *     summary: bool,
     *     memory: bool,
     *     settings: bool,
     *     wal: bool,
     *     format: string
     * } $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            analyze: $data['analyze'],
            verbose: $data['verbose'],
            costs: $data['costs'],
            buffers: $data['buffers'],
            timing: $data['timing'],
            summary: $data['summary'],
            memory: $data['memory'],
            settings: $data['settings'],
            wal: $data['wal'],
            format: ExplainFormat::from($data['format']),
        );
    }

    /**
     * @return array{
     *     analyze: bool,
     *     verbose: bool,
     *     costs: bool,
     *     buffers: bool,
     *     timing: bool,
     *     summary: bool,
     *     memory: bool,
     *     settings: bool,
     *     wal: bool,
     *     format: string
     * }
     */
    public function normalize(): array
    {
        return [
            'analyze' => $this->analyze,
            'verbose' => $this->verbose,
            'costs' => $this->costs,
            'buffers' => $this->buffers,
            'timing' => $this->timing,
            'summary' => $this->summary,
            'memory' => $this->memory,
            'settings' => $this->settings,
            'wal' => $this->wal,
            'format' => $this->format->value,
        ];
    }

    public function withAnalyze(): self
    {
        return new self(
            analyze: true,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withBuffers(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: true,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withCosts(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: true,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withFormat(ExplainFormat $format): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $format,
        );
    }

    public function withMemory(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: true,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withoutAnalyze(): self
    {
        return new self(
            analyze: false,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: false,
            timing: false,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: false,
            format: $this->format,
        );
    }

    public function withoutBuffers(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: false,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withoutCosts(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: false,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withoutMemory(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: false,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withoutSettings(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: false,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withoutSummary(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: false,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withoutTiming(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: false,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withoutVerbose(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: false,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withoutWal(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: false,
            format: $this->format,
        );
    }

    public function withSettings(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: true,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withSummary(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: true,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withTiming(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: true,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withVerbose(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: true,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: $this->wal,
            format: $this->format,
        );
    }

    public function withWal(): self
    {
        return new self(
            analyze: $this->analyze,
            verbose: $this->verbose,
            costs: $this->costs,
            buffers: $this->buffers,
            timing: $this->timing,
            summary: $this->summary,
            memory: $this->memory,
            settings: $this->settings,
            wal: true,
            format: $this->format,
        );
    }

    private function validate(): void
    {
        if (!$this->analyze) {
            if ($this->buffers) {
                throw InvalidExplainConfigException::buffersRequiresAnalyze();
            }

            if ($this->timing) {
                throw InvalidExplainConfigException::timingRequiresAnalyze();
            }

            if ($this->wal) {
                throw InvalidExplainConfigException::walRequiresAnalyze();
            }
        }
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Transformers;

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
    }

    public static function forAnalysis() : self
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

    public static function forEstimate() : self
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
}

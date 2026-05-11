<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Analyzer;

enum InsightSeverity: string
{
    case CRITICAL = 'critical';
    case INFO = 'info';
    case WARNING = 'warning';
}

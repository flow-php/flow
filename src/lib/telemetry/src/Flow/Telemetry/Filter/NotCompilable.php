<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use RuntimeException;

/**
 * Control flow: CompiledMatcher catches it and falls back to interpreted matching.
 */
final class NotCompilable extends RuntimeException {}

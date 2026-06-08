<?php

declare(strict_types=1);

namespace Flow\Telemetry\Filter;

use RuntimeException;

/**
 * Thrown by {@see CompilableMatcher::compile()} when a matcher cannot be inlined
 * as PHP. It is an internal control-flow signal: {@see CompiledMatcher} catches
 * it and falls back to interpreted matching.
 */
final class NotCompilable extends RuntimeException {}

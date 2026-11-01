<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Middleware;

use Closure;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Filter\AttributeFilter;
use Flow\Telemetry\Filter\AttributeSource;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogMiddleware;

final readonly class AttributeFilteringLogMiddleware implements LogMiddleware
{
    public const string SEVERITY_KEY = 'log.severity';

    public const string SEVERITY_NAME_KEY = 'log.severity_name';

    /**
     * @var Closure(Attributes ...): bool
     */
    private Closure $shouldDrop;

    /**
     * @var non-empty-list<AttributeSource>
     */
    private array $sources;

    public function __construct(AttributeFilter $filter)
    {
        $this->shouldDrop = $filter->dropFunction();
        $this->sources = $filter->sources();
    }

    public function process(LogEntry $entry): ?LogEntry
    {
        if (($this->shouldDrop)(...$this->attributesFor($entry))) {
            return null;
        }

        return $entry;
    }

    /**
     * @return non-empty-list<Attributes>
     */
    private function attributesFor(LogEntry $entry): array
    {
        return AttributeSource::select(
            $this->sources,
            $entry
                ->record
                ->attributes
                ->with(self::SEVERITY_KEY, $entry->record->severity->value)
                ->with(self::SEVERITY_NAME_KEY, $entry->record->severity->name()),
            $entry->resource->attributes,
            $entry->scope->attributes,
        );
    }
}

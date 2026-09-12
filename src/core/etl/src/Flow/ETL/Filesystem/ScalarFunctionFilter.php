<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Schema;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter;
use Throwable;

use function Flow\ETL\DSL\row;
use function sprintf;

final readonly class ScalarFunctionFilter implements Filter
{
    private ScalarFunction $resolved;

    public function __construct(
        ScalarFunction $function,
        private Schema $partitions,
        private FlowContext $context,
    ) {
        // The comparability gate belongs here, where the function and the schema it binds against
        // are both in hand - a filter reaching withPathFilter() any other way is gated too.
        // resolved() guards it: files() and from_path_partitions() declare no partition columns, so
        // their reference never resolves and returns() would throw instead of answering.
        $this->resolved = (new ReferenceResolver())->resolve($function, $partitions);

        if ($this->resolved->resolved()) {
            $this->resolved->returns();
        }
    }

    public function accept(FileStatus $status): bool
    {
        $values = [];

        foreach ($status->path->partitions()->toArray() as $partition) {
            // findDefinition(), not get(): files() and from_path_partitions() declare no partition
            // columns and filterPartitions() works on both, while Schema::get() would throw.
            $definition = $this->partitions->findDefinition($partition->name);

            if ($definition === null || $definition->matches($partition->value)) {
                $values[$partition->name] = $partition->value;

                continue;
            }

            try {
                $values[$partition->name] = $definition->type()->cast($partition->value);
            } catch (Throwable $e) {
                throw new InvalidArgumentException(
                    sprintf(
                        'Partition column "%s" of file "%s" declares type %s, but its path value %s cannot be cast to it.',
                        $partition->name,
                        $status->path->uri(),
                        $definition->type()->toString(),
                        $partition->value === null ? 'NULL' : '"' . $partition->value . '"',
                    ),
                    0,
                    $e,
                );
            }
        }

        // @mago-ignore analysis:mixed-operand
        return (bool) $this->resolved->eval(row($values), $this->context);
    }
}

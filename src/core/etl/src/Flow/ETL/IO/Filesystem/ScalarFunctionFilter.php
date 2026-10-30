<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Filesystem;

use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor\File\FileColumns;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter;

final readonly class ScalarFunctionFilter implements Filter
{
    private ScalarFunction $resolved;

    public function __construct(
        private ScalarFunction $function,
        private Schema $partitions,
        private Backend $backend,
    ) {
        // The comparability gate belongs here, where the function and the schema it binds against
        // are both in hand - a filter reaching a Read any other way is gated too.
        // resolved() guards it: files() and from_path_partitions() declare no partition columns, so
        // their reference never resolves and returns() would throw instead of answering.
        $this->resolved = (new ReferenceResolver())->resolve($function, $partitions);

        if ($this->resolved->resolved()) {
            $this->resolved->returns();
        }
    }

    public function function(): ScalarFunction
    {
        return $this->function;
    }

    public function accept(FileStatus $status): bool
    {
        $values = [];
        $definitions = [];

        foreach ($status->path->partitions()->toArray() as $partition) {
            // findDefinition(), not get(): a source that declares no partition columns hands an empty schema
            // here, and Schema::get() would throw.
            $definition = $this->partitions->findDefinition($partition->name);
            $definitions[] = $definition ?? new StringDefinition($partition->name, nullable: true);
            $values[$partition->name] = $definition === null
                ? $partition->value
                : FileColumns::partitionValue($definition, $partition->name, $partition->value, $status->path);
        }

        $schema = new Schema(...$definitions);

        // an undeclared partition column resolves against the file's own path value (a str column)
        // @mago-ignore analysis:mixed-operand
        return (bool) (new ReferenceResolver())
            ->resolve($this->resolved, $schema)
            ->eval((new RowsBuilder($schema, $this->backend))->appendRecords([$values])->finish(), $this->backend)
            ->value(0);
    }
}

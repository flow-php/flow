<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\AlterSequence;

interface AlterSequenceOptionsStep extends AlterSequenceFinalStep
{
    public function asType(string $dataType): self;

    public function cache(int $cache): self;

    public function cycle(): self;

    public function incrementBy(int $increment): self;

    public function maxValue(int $maxValue): self;

    public function minValue(int $minValue): self;

    public function noCycle(): self;

    public function noMaxValue(): self;

    public function noMinValue(): self;

    public function ownedBy(string $table, string $column): self;

    public function ownedByNone(): self;

    public function ownerTo(string $owner): AlterSequenceOwnerFinalStep;

    public function renameTo(string $newName): RenameSequenceFinalStep;

    public function restart(): self;

    public function restartWith(int $restart): self;

    public function setLogged(): AlterSequenceLoggingFinalStep;

    public function setSchema(string $schema): AlterSequenceSchemaFinalStep;

    public function setUnlogged(): AlterSequenceLoggingFinalStep;

    public function startWith(int $start): self;
}

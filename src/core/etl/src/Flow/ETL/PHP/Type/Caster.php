<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Caster\ArrayCastingHandler;
use Flow\ETL\PHP\Type\Caster\BooleanCastingHandler;
use Flow\ETL\PHP\Type\Caster\CastingContext;
use Flow\ETL\PHP\Type\Caster\CastingHandler;
use Flow\ETL\PHP\Type\Caster\DateTimeCastingHandler;
use Flow\ETL\PHP\Type\Caster\EnumCastingHandler;
use Flow\ETL\PHP\Type\Caster\FloatCastingHandler;
use Flow\ETL\PHP\Type\Caster\IntegerCastingHandler;
use Flow\ETL\PHP\Type\Caster\JsonCastingHandler;
use Flow\ETL\PHP\Type\Caster\ListCastingHandler;
use Flow\ETL\PHP\Type\Caster\MapCastingHandler;
use Flow\ETL\PHP\Type\Caster\NullCastingHandler;
use Flow\ETL\PHP\Type\Caster\ObjectCastingHandler;
use Flow\ETL\PHP\Type\Caster\StringCastingHandler;
use Flow\ETL\PHP\Type\Caster\StructureCastingHandler;
use Flow\ETL\PHP\Type\Caster\UuidCastingHandler;
use Flow\ETL\PHP\Type\Caster\XMLCastingHandler;

final class Caster
{
    /**
     * @param array<CastingHandler> $handlers
     */
    public function __construct(private readonly array $handlers)
    {
    }

    public static function default() : self
    {
        return new self([
            new StringCastingHandler(),
            new IntegerCastingHandler(),
            new BooleanCastingHandler(),
            new FloatCastingHandler(),
            new XMLCastingHandler(),
            new UuidCastingHandler(),
            new ObjectCastingHandler(),
            new DateTimeCastingHandler(),
            new JsonCastingHandler(),
            new ArrayCastingHandler(),
            new ListCastingHandler(),
            new MapCastingHandler(),
            new StructureCastingHandler(),
            new NullCastingHandler(),
            new EnumCastingHandler(),
        ]);
    }

    public function to(Type $type) : CastingContext
    {
        foreach ($this->handlers as $handler) {
            if ($handler->supports($type)) {
                return new CastingContext($handler, $type);
            }
        }

        throw new RuntimeException("There is no casting handler for \"{$type->toString()}\" type");
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use function Flow\ETL\DSL\type_array;
use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_integer;
use function Flow\ETL\DSL\type_json;
use function Flow\ETL\DSL\type_null;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_uuid;
use function Flow\ETL\DSL\type_xml;
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
            type_string()->toString() => new StringCastingHandler(),
            type_integer()->toString() => new IntegerCastingHandler(),
            type_boolean()->toString() => new BooleanCastingHandler(),
            type_float()->toString() => new FloatCastingHandler(),
            type_xml()->toString() => new XMLCastingHandler(),
            type_uuid()->toString() => new UuidCastingHandler(),
            'object' => new ObjectCastingHandler(),
            type_datetime()->toString() => new DateTimeCastingHandler(),
            type_json()->toString() => new JsonCastingHandler(),
            type_array()->toString() => new ArrayCastingHandler(),
            'list' => new ListCastingHandler(),
            'map', new MapCastingHandler(),
            'structure', new StructureCastingHandler(),
            type_null()->toString() => new NullCastingHandler(),
            'enum' => new EnumCastingHandler(),
        ]);
    }

    public function to(Type $type) : CastingContext
    {
        if (\array_key_exists($type->toString(), $this->handlers)) {
            return new CastingContext($this->handlers[$type->toString()], $type);
        }

        foreach ($this->handlers as $handler) {
            if ($handler->supports($type)) {
                return new CastingContext($handler, $type);
            }
        }

        throw new RuntimeException("There is no casting handler for \"{$type->toString()}\" type");
    }
}

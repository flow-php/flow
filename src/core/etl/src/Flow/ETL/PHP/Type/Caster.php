<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use function Flow\ETL\DSL\{type_array,
    type_boolean,
    type_datetime,
    type_float,
    type_integer,
    type_json,
    type_null,
    type_string,
    type_uuid,
    type_xml
    };
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Caster\{ArrayCastingHandler, BooleanCastingHandler, CastingContext, CastingHandler, DateTimeCastingHandler, EnumCastingHandler, FloatCastingHandler, IntegerCastingHandler, JsonCastingHandler, ListCastingHandler, MapCastingHandler, NullCastingHandler, ObjectCastingHandler, StringCastingHandler, StructureCastingHandler, UuidCastingHandler, XMLCastingHandler};

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
            'map' => new MapCastingHandler(),
            'structure' => new StructureCastingHandler(),
            type_null()->toString() => new NullCastingHandler(),
            'enum' => new EnumCastingHandler(),
        ]);
    }

    public function to(Type $type) : CastingContext
    {
        if (\array_key_exists($type->toString(), $this->handlers)) {
            return new CastingContext($this->handlers[$type->toString()], $type, $this);
        }

        foreach ($this->handlers as $handler) {
            if ($handler->supports($type)) {
                return new CastingContext($handler, $type, $this);
            }
        }

        throw new RuntimeException("There is no casting handler for \"{$type->toString()}\" type");
    }
}

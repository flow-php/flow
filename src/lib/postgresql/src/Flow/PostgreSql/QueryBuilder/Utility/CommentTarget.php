<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\ObjectType;

enum CommentTarget : int
{
    case COLUMN = ObjectType::OBJECT_COLUMN;
    case DATABASE = ObjectType::OBJECT_DATABASE;
    case EXTENSION = ObjectType::OBJECT_EXTENSION;
    case FUNCTION = ObjectType::OBJECT_FUNCTION;
    case INDEX = ObjectType::OBJECT_INDEX;
    case MATERIALIZED_VIEW = ObjectType::OBJECT_MATVIEW;
    case PROCEDURE = ObjectType::OBJECT_PROCEDURE;
    case ROLE = ObjectType::OBJECT_ROLE;
    case SCHEMA = ObjectType::OBJECT_SCHEMA;
    case SEQUENCE = ObjectType::OBJECT_SEQUENCE;
    case TABLE = ObjectType::OBJECT_TABLE;
    case TRIGGER = ObjectType::OBJECT_TRIGGER;
    case TYPE = ObjectType::OBJECT_TYPE;
    case VIEW = ObjectType::OBJECT_VIEW;
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Role;

enum RoleOption: string
{
    case BYPASSRLS = 'bypassrls';
    case CREATEDB = 'createdb';
    case CREATEROLE = 'createrole';
    case INHERIT = 'inherit';
    case LOGIN = 'canlogin';
    case NOBYPASSRLS = 'nobypassrls';
    case NOCREATEDB = 'nocreatedb';
    case NOCREATEROLE = 'nocreaterole';
    case NOINHERIT = 'noinherit';
    case NOLOGIN = 'nologin';
    case NOREPLICATION = 'noreplication';
    case NOSUPERUSER = 'nosuperuser';
    case REPLICATION = 'replication';
    case SUPERUSER = 'superuser';
}

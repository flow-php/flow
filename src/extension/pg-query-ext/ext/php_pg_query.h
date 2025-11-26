/**
 * PHP pg_query extension header
 *
 * @link https://github.com/flow-php/flow
 */

#ifndef PHP_PG_QUERY_H
#define PHP_PG_QUERY_H

extern zend_module_entry pg_query_module_entry;
#define phpext_pg_query_ptr &pg_query_module_entry

#define PHP_PG_QUERY_VERSION "0.1.0"
#define PHP_PG_QUERY_EXTNAME "pg_query"

#ifdef PHP_WIN32
#   define PHP_PG_QUERY_API __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#   define PHP_PG_QUERY_API __attribute__ ((visibility("default")))
#else
#   define PHP_PG_QUERY_API
#endif

#ifdef ZTS
#include "TSRM.h"
#endif

PHP_MINIT_FUNCTION(pg_query);
PHP_MSHUTDOWN_FUNCTION(pg_query);
PHP_RINIT_FUNCTION(pg_query);
PHP_RSHUTDOWN_FUNCTION(pg_query);
PHP_MINFO_FUNCTION(pg_query);

PHP_FUNCTION(pg_query_parse);
PHP_FUNCTION(pg_query_fingerprint);
PHP_FUNCTION(pg_query_normalize);
PHP_FUNCTION(pg_query_parse_plpgsql);
PHP_FUNCTION(pg_query_split);
PHP_FUNCTION(pg_query_scan);

#endif /* PHP_PG_QUERY_H */

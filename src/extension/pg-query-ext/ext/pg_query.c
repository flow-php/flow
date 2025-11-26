/**
 * PHP pg_query extension implementation
 *
 * Provides PostgreSQL query parsing capabilities using libpg_query
 *
 * @link https://github.com/flow-php/flow
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <string.h>

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_pg_query.h"
#include "pg_query.h"
#include "zend_exceptions.h"
#include "ext/spl/spl_exceptions.h"

#include "pg_query_arginfo.h"

zend_module_entry pg_query_module_entry = {
    STANDARD_MODULE_HEADER,
    PHP_PG_QUERY_EXTNAME,
    ext_functions,
    PHP_MINIT(pg_query),
    PHP_MSHUTDOWN(pg_query),
    PHP_RINIT(pg_query),
    PHP_RSHUTDOWN(pg_query),
    PHP_MINFO(pg_query),
    PHP_PG_QUERY_VERSION,
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_PG_QUERY
#ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE()
#endif
ZEND_GET_MODULE(pg_query)
#endif

PHP_MINIT_FUNCTION(pg_query)
{
    pg_query_init();
    return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(pg_query)
{
    return SUCCESS;
}

PHP_RINIT_FUNCTION(pg_query)
{
#if defined(ZTS) && defined(COMPILE_DL_PG_QUERY)
    ZEND_TSRMLS_CACHE_UPDATE();
#endif
    return SUCCESS;
}

PHP_RSHUTDOWN_FUNCTION(pg_query)
{
    return SUCCESS;
}

PHP_MINFO_FUNCTION(pg_query)
{
    php_info_print_table_start();
    php_info_print_table_header(2, "pg_query support", "enabled");
    php_info_print_table_row(2, "Version", PHP_PG_QUERY_VERSION);
    php_info_print_table_row(2, "libpg_query version", "17-6.1.0");
    php_info_print_table_end();
}

PHP_FUNCTION(pg_query_parse)
{
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    PgQueryParseResult result = pg_query_parse(sql);

    if (result.error) {
        char error_msg[1024];
        int cursor_pos = result.error->cursorpos;

        if (result.error->message) {
            strncpy(error_msg, result.error->message, sizeof(error_msg) - 1);
            error_msg[sizeof(error_msg) - 1] = '\0';
        } else {
            strcpy(error_msg, "Unknown parse error");
        }

        pg_query_free_parse_result(result);

        zend_throw_exception_ex(spl_ce_RuntimeException, cursor_pos, "%s", error_msg);
        RETURN_THROWS();
    }

    zend_string *json = zend_string_init(result.parse_tree, strlen(result.parse_tree), 0);
    pg_query_free_parse_result(result);

    RETURN_STR(json);
}

PHP_FUNCTION(pg_query_fingerprint)
{
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    PgQueryFingerprintResult result = pg_query_fingerprint(sql);

    if (result.error) {
        pg_query_free_fingerprint_result(result);
        RETURN_FALSE;
    }

    zend_string *fingerprint = zend_string_init(result.fingerprint_str,
                                                strlen(result.fingerprint_str), 0);
    pg_query_free_fingerprint_result(result);

    RETURN_STR(fingerprint);
}

PHP_FUNCTION(pg_query_normalize)
{
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    PgQueryNormalizeResult result = pg_query_normalize(sql);

    if (result.error) {
        pg_query_free_normalize_result(result);
        RETURN_FALSE;
    }

    zend_string *normalized = zend_string_init(result.normalized_query,
                                              strlen(result.normalized_query), 0);
    pg_query_free_normalize_result(result);

    RETURN_STR(normalized);
}

PHP_FUNCTION(pg_query_parse_plpgsql)
{
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    PgQueryPlpgsqlParseResult result = pg_query_parse_plpgsql(sql);

    if (result.error) {
        char error_msg[1024];

        if (result.error->message) {
            strncpy(error_msg, result.error->message, sizeof(error_msg) - 1);
            error_msg[sizeof(error_msg) - 1] = '\0';
        } else {
            strcpy(error_msg, "PL/pgSQL parse error");
        }

        pg_query_free_plpgsql_parse_result(result);

        zend_throw_exception(spl_ce_RuntimeException, error_msg, 0);
        RETURN_THROWS();
    }

    zend_string *json = zend_string_init(result.plpgsql_funcs,
                                        strlen(result.plpgsql_funcs), 0);
    pg_query_free_plpgsql_parse_result(result);

    RETURN_STR(json);
}

PHP_FUNCTION(pg_query_split)
{
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    PgQuerySplitResult result = pg_query_split_with_scanner(sql);

    if (result.error) {
        char error_msg[1024];

        if (result.error->message) {
            strncpy(error_msg, result.error->message, sizeof(error_msg) - 1);
            error_msg[sizeof(error_msg) - 1] = '\0';
        } else {
            strcpy(error_msg, "Split error");
        }

        pg_query_free_split_result(result);

        zend_throw_exception(spl_ce_RuntimeException, error_msg, 0);
        RETURN_THROWS();
    }

    array_init(return_value);

    for (int i = 0; i < result.n_stmts; i++) {
        int stmt_len = result.stmts[i]->stmt_len;
        int stmt_location = result.stmts[i]->stmt_location;

        if (stmt_len == 0 && i == result.n_stmts - 1) {
            stmt_len = sql_len - stmt_location;
        }

        zend_string *stmt = zend_string_init(sql + stmt_location, stmt_len, 0);
        add_next_index_str(return_value, stmt);
    }

    pg_query_free_split_result(result);
}

PHP_FUNCTION(pg_query_scan)
{
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    PgQueryScanResult result = pg_query_scan(sql);

    if (result.error) {
        char error_msg[1024];

        if (result.error->message) {
            strncpy(error_msg, result.error->message, sizeof(error_msg) - 1);
            error_msg[sizeof(error_msg) - 1] = '\0';
        } else {
            strcpy(error_msg, "Scan error");
        }

        pg_query_free_scan_result(result);

        zend_throw_exception(spl_ce_RuntimeException, error_msg, 0);
        RETURN_THROWS();
    }

    zend_string *protobuf_data = NULL;
    if (result.pbuf.len > 0 && result.pbuf.data != NULL) {
        protobuf_data = zend_string_init(result.pbuf.data, result.pbuf.len, 0);
    } else {
        protobuf_data = zend_string_init("", 0, 0);
    }

    pg_query_free_scan_result(result);

    RETURN_STR(protobuf_data);
}
